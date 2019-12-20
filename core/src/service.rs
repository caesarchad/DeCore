//! The `service` module implements a trait used by services and phases.
//!
//! A Service is any object that implements its functionality on a separate thread. It implements a
//! `join()` method, which can be used to wait for that thread to close.
//!
//! The Service trait may also be used to implement a pipeline phase. Like a service, its
//! functionality is also implemented by a thread, but unlike a service, a phase isn't a server
//! that replies to client requests. Instead, a phase acts more like a pure function. It's a oneway
//! street. It processes messages from its input channel and then sends the processed data to an
//! output channel. Phases can be composed to form a linear chain called a pipeline.
//!
//! The approach to creating a pipeline phase in Rust may be unique to Morgan. We haven't seen the
//! same technique used in other Rust projects and there may be better ways to do it. The Morgan
//! approach defines a phase as an object that communicates to its previous phase and the next
//! phase using channels. By convention, each phase accepts a *receiver* for input and creates a
//! second output channel. The second channel is used to pass data to the next phase, and so its
//! sender is moved into the phase's thread and the receiver is returned from its constructor.
//!
//! A well-written phase should create a thread and call a short `run()` method.  The method should
//! read input from its input channel, call a function from another module that processes it, and
//! then send the output to the output channel. The functionality in the second module will likely
//! not use threads or channels.

use std::thread::Result;

pub trait Service {
    type JoinReturnType;

    fn join(self) -> Result<Self::JoinReturnType>;
}
