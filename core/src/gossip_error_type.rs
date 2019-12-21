#[derive(PartialEq, Debug)]
pub enum NodeTbleErr {
    NoPeers,
    PushMessageTimeout,
    PushMessagePrune,
    PushMessageOldVersion,
    BadPruneDestination,
    PruneMessageTimeout,
}
