use crate::endpoint::{Endpoint};
use crate::resource_id::{ResourceId, ResourceType, ResourceIdGenerator};
use crate::util::{OTHER_THREAD_ERR, SendingStatus};

use mio::net::{TcpListener, TcpStream};
use mio::{Poll, Interest, Token, Events, Registry};

use tungstenite::server::{accept as ws_accept};
use tungstenite::protocol::{Role, WebSocket, Message};
use tungstenite::error::{Error};

use std::net::{SocketAddr, TcpStream as StdTcpStream};
use std::time::{Duration};
use std::collections::{HashMap, hash_map::Entry};
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::io::{self, ErrorKind};

const INPUT_BUFFER_SIZE: usize = 65536;
const NETWORK_SAMPLING_TIMEOUT: u64 = 50; //ms
const EVENTS_SIZE: usize = 1024;

#[derive(Debug)]
pub enum WsEvent<'a> {
    Connection,
    Data(&'a [u8]),
    Disconnection,
}

struct Store {
    // We store the addr because we will need it when the stream crash.
    // When a stream crash by an error (i.e. reset) peer_addr no longer returns the addr.
    ws_sockets: RwLock<HashMap<ResourceId, (WebSocket<TcpStream>, SocketAddr)>>,
    listeners: RwLock<HashMap<ResourceId, TcpListener>>,
    id_generator: ResourceIdGenerator,
    registry: Registry,
}

impl Store {
    fn new(id_generator: ResourceIdGenerator, registry: Registry) -> Store {
        Store {
            ws_sockets: RwLock::new(HashMap::new()),
            listeners: RwLock::new(HashMap::new()),
            id_generator,
            registry,
        }
    }
}

pub struct WsAdapter {
    thread: Option<JoinHandle<()>>,
    thread_running: Arc<AtomicBool>,
    store: Arc<Store>,
}

impl WsAdapter {
    pub fn init<C>(adapter_id: u8, mut event_callback: C) -> Self
    where C: for<'b> FnMut(Endpoint, WsEvent<'b>) + Send + 'static {
        let id_generator = ResourceIdGenerator::new(adapter_id);
        let poll = Poll::new().unwrap();
        let store = Store::new(id_generator, poll.registry().try_clone().unwrap());
        let store = Arc::new(store);
        let thread_store = store.clone();

        let thread_running = Arc::new(AtomicBool::new(true));
        let running = thread_running.clone();

        let thread = thread::Builder::new()
            .name("message-io: ws-adapter".into())
            .spawn(move || {
                let timeout = Some(Duration::from_millis(NETWORK_SAMPLING_TIMEOUT));
                let mut event_processor = WsEventProcessor::new(thread_store, timeout, poll);

                while running.load(Ordering::Relaxed) {
                    event_processor.process(&mut event_callback);
                }
            })
            .unwrap();

        Self { thread: Some(thread), thread_running, store }
    }

    pub fn connect(&mut self, addr: SocketAddr) -> io::Result<Endpoint> {
        let stream = StdTcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        let stream = TcpStream::from_std(stream);
        let mut ws_socket = WebSocket::from_raw_socket(stream, Role::Client, None);

        let id = self.store.id_generator.generate(ResourceType::Remote);
        self.store
            .registry
            .register(ws_socket.get_mut(), Token(id.raw()), Interest::READABLE)
            .unwrap();
        self.store.ws_sockets.write().expect(OTHER_THREAD_ERR).insert(id, (ws_socket, addr));
        Ok(Endpoint::new(id, addr))
    }

    pub fn listen(&mut self, addr: SocketAddr) -> io::Result<(ResourceId, SocketAddr)> {
        let mut listener = TcpListener::bind(addr)?;

        let id = self.store.id_generator.generate(ResourceType::Listener);
        let real_addr = listener.local_addr().unwrap();
        self.store.registry.register(&mut listener, Token(id.raw()), Interest::READABLE).unwrap();
        self.store.listeners.write().expect(OTHER_THREAD_ERR).insert(id, listener);
        Ok((id, real_addr))
    }

    pub fn remove(&mut self, id: ResourceId) -> Option<()> {
        match id.resource_type() {
            ResourceType::Listener => {
                self.store.listeners.write().expect(OTHER_THREAD_ERR).remove(&id).map(
                    |mut listener| {
                        self.store.registry.deregister(&mut listener).unwrap();
                    },
                )
            }
            ResourceType::Remote => {
                self.store.ws_sockets.write().expect(OTHER_THREAD_ERR).remove(&id).map(
                    |(mut socket, _)| {
                        self.store.registry.deregister(socket.get_mut()).unwrap();
                    },
                )
            }
        }
    }

    pub fn local_address(&self, id: ResourceId) -> Option<SocketAddr> {
        match id.resource_type() {
            ResourceType::Listener => self
                .store
                .listeners
                .read()
                .expect(OTHER_THREAD_ERR)
                .get(&id)
                .map(|listener| listener.local_addr().unwrap()),
            ResourceType::Remote => self
                .store
                .ws_sockets
                .read()
                .expect(OTHER_THREAD_ERR)
                .get(&id)
                .map(|(ws_socket, _)| ws_socket.get_ref().local_addr().unwrap()),
        }
    }

    pub fn send(&mut self, endpoint: Endpoint, data: &[u8]) -> SendingStatus {
        assert_eq!(endpoint.resource_id().adapter_id(), self.store.id_generator.adapter_id());

        match self
            .store
            .ws_sockets
            .write()
            .expect(OTHER_THREAD_ERR)
            .get_mut(&endpoint.resource_id())
        {
            Some((socket, _)) => {
                let message = Message::Binary(data.to_vec());
                let mut result = socket.write_message(message);

                loop {
                    match result {
                        Ok(_) => break SendingStatus::Sent,
                        Err(error) => match error {
                            Error::Io(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                                result = socket.write_pending();
                            }
                            Error::ConnectionClosed => break SendingStatus::RemovedEndpoint,
                            _ => break SendingStatus::RemovedEndpoint,
                        },
                    }
                }
            }
            None => SendingStatus::RemovedEndpoint,
        }
    }
}

impl Drop for WsAdapter {
    fn drop(&mut self) {
        self.thread_running.store(false, Ordering::Relaxed);
        self.thread.take().unwrap().join().expect(OTHER_THREAD_ERR);
    }
}

struct WsEventProcessor {
    resource_processor: WsResourceProcessor,
    timeout: Option<Duration>,
    poll: Poll,
    events: Events,
}

impl WsEventProcessor {
    fn new(store: Arc<Store>, timeout: Option<Duration>, poll: Poll) -> WsEventProcessor {
        Self {
            resource_processor: WsResourceProcessor::new(store),
            timeout,
            poll,
            events: Events::with_capacity(EVENTS_SIZE),
        }
    }

    pub fn process<C>(&mut self, event_callback: &mut C)
    where C: for<'b> FnMut(Endpoint, WsEvent<'b>) {
        loop {
            match self.poll.poll(&mut self.events, self.timeout) {
                Ok(_) => break self.process_resource(event_callback),
                Err(e) => match e.kind() {
                    ErrorKind::Interrupted => continue,
                    _ => Err(e).unwrap(),
                },
            }
        }
    }

    fn process_resource<C>(&mut self, event_callback: &mut C)
    where C: for<'b> FnMut(Endpoint, WsEvent<'b>) {
        for mio_event in &self.events {
            let id = ResourceId::from(mio_event.token().0);

            log::trace!("Wake from poll for WebSocket with resource id {}. ", id);

            match id.resource_type() {
                ResourceType::Listener => {
                    self.resource_processor.process_listener(id, event_callback)
                }
                ResourceType::Remote => self.resource_processor.process_socket(id, event_callback),
            }
        }
    }
}

struct WsResourceProcessor {
    store: Arc<Store>,
}

impl WsResourceProcessor {
    fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    fn process_listener<C>(&mut self, id: ResourceId, event_callback: &mut C)
    where C: for<'b> FnMut(Endpoint, WsEvent<'b>) {
        // We check the existance of the listener because some event could be produced
        // before removing it.
        if let Some(listener) = self.store.listeners.read().expect(OTHER_THREAD_ERR).get(&id) {
            let mut ws_sockets = self.store.ws_sockets.write().expect(OTHER_THREAD_ERR);
            loop {
                match listener.accept() {
                    Ok((mut stream, addr)) => {
                        let id = self.store.id_generator.generate(ResourceType::Remote);
                        self.store
                            .registry
                            .register(&mut stream, Token(id.raw()), Interest::READABLE)
                            .unwrap();

                        let ws_socket = ws_accept(stream).unwrap();
                        ws_sockets.insert(id, (ws_socket, addr));

                        let endpoint = Endpoint::new(id, addr);
                        event_callback(endpoint, WsEvent::Connection);
                    }
                    Err(ref err) if err.kind() == ErrorKind::WouldBlock => break,
                    Err(ref err) if err.kind() == ErrorKind::Interrupted => continue,
                    Err(_) => break, // should not happened
                }
            }
        }
    }

    fn process_socket<C>(&mut self, id: ResourceId, event_callback: &mut C)
    where C: for<'b> FnMut(Endpoint, WsEvent<'b>) {
        let mut ws_sockets = self.store.ws_sockets.write().expect(OTHER_THREAD_ERR);
        if let Entry::Occupied(mut entry) = ws_sockets.entry(id) {
            let (socket, addr) = entry.get_mut();
            let endpoint = Endpoint::new(id, *addr);
            let must_be_removed = loop {
                let result = socket.read_message();
                match result {
                    Ok(message) => match message {
                        Message::Binary(data) => event_callback(endpoint, WsEvent::Data(&data)),
                        Message::Close(_) => break true,
                        _ => continue,
                    },
                    Err(Error::Io(ref err)) if err.kind() == io::ErrorKind::WouldBlock => {
                        break false
                    }
                    Err(Error::Io(ref err)) if err.kind() == ErrorKind::Interrupted => continue,
                    _ => break true,
                }
            };
            if must_be_removed {
                self.store.registry.deregister(socket.get_mut()).unwrap();
                entry.remove();
                event_callback(endpoint, WsEvent::Disconnection);
            }
        };
    }
}
