use crate::connection_info::ContactInfo;
use bincode::serialize;
use morgan_interface::bvm_address::BvmAddr;
use morgan_interface::signature::{Keypair, Signable, Signature};
use morgan_interface::transaction::Transaction;
use std::{collections::BTreeSet, time::Duration};
use std::fmt;
use prometheus::{
    core::{Collector, Desc},
    proto::MetricFamily,
    Histogram, HistogramOpts, HistogramTimer, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts,
};

/// ContInfTblValue that is replicated across the cluster
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ContInfTblValue {
    /// * Merge Strategy - Latest wallclock is picked
    ContactInfo(ContactInfo),
    /// * Merge Strategy - Latest wallclock is picked
    Vote(Vote),
    /// * Merge Strategy - Latest wallclock is picked
    EpochSlots(EpochSlots),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EpochSlots {
    pub from: BvmAddr,
    pub root: u64,
    pub slots: BTreeSet<u64>,
    pub signature: Signature,
    pub wallclock: u64,
}

impl EpochSlots {
    pub fn new(from: BvmAddr, root: u64, slots: BTreeSet<u64>, wallclock: u64) -> Self {
        Self {
            from,
            root,
            slots,
            signature: Signature::default(),
            wallclock,
        }
    }
}

impl Signable for EpochSlots {
    fn address(&self) -> BvmAddr {
        self.from
    }

    fn signable_data(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct SignData<'a> {
            root: u64,
            slots: &'a BTreeSet<u64>,
            wallclock: u64,
        }
        let data = SignData {
            root: self.root,
            slots: &self.slots,
            wallclock: self.wallclock,
        };
        serialize(&data).expect("unable to serialize EpochSlots")
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature;
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Vote {
    pub from: BvmAddr,
    pub transaction: Transaction,
    pub signature: Signature,
    pub wallclock: u64,
}

impl Vote {
    pub fn new(from: &BvmAddr, transaction: Transaction, wallclock: u64) -> Self {
        Self {
            from: *from,
            transaction,
            signature: Signature::default(),
            wallclock,
        }
    }
}

impl Signable for Vote {
    fn address(&self) -> BvmAddr {
        self.from
    }

    fn signable_data(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct SignData<'a> {
            transaction: &'a Transaction,
            wallclock: u64,
        }
        let data = SignData {
            transaction: &self.transaction,
            wallclock: self.wallclock,
        };
        serialize(&data).expect("unable to serialize Vote")
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature
    }
}

pub struct DurationHistogram {
    histogram: Histogram,
}

impl DurationHistogram {
    pub fn new(histogram: Histogram) -> DurationHistogram {
        DurationHistogram { histogram }
    }

    pub fn observe_duration(&self, d: Duration) {
        // Duration is full seconds + nanos elapsed from the presious full second
        let v = d.as_secs() as f64 + f64::from(d.subsec_nanos()) / 1e9;
        self.histogram.observe(v);
    }
}

#[derive(Clone)]
pub struct OpMetrics {
    module: String,
    counters: IntCounterVec,
    gauges: IntGaugeVec,
    peer_gauges: IntGaugeVec,
    duration_histograms: HistogramVec,
}

impl OpMetrics {
    pub fn new<S: Into<String>>(name: S) -> OpMetrics {
        let name_str = name.into();
        OpMetrics {
            module: name_str.clone(),
            counters: IntCounterVec::new(
                Opts::new(name_str.clone(), format!("Counters for {}", name_str)),
                &["op"],
            )
            .unwrap(),
            gauges: IntGaugeVec::new(
                Opts::new(
                    format!("{}_gauge", name_str.clone()),
                    format!("Gauges for {}", name_str),
                ),
                &["op"],
            )
            .unwrap(),
            peer_gauges: IntGaugeVec::new(
                Opts::new(
                    format!("{}_peer_gauge", name_str.clone()),
                    format!("Gauges of each remote peer for {}", name_str),
                ),
                &["op", "remote_peer_id"],
            )
            .unwrap(),
            duration_histograms: HistogramVec::new(
                HistogramOpts::new(
                    format!("{}_duration", name_str.clone()),
                    format!("Histogram values for {}", name_str),
                ),
                &["op"],
            )
            .unwrap(),
        }
    }

    pub fn new_and_registered<S: Into<String>>(name: S) -> OpMetrics {
        let op_metrics = OpMetrics::new(name);
        prometheus::register(Box::new(op_metrics.clone()))
            .expect("OpMetrics registration on Prometheus failed.");
        op_metrics
    }

    #[inline]
    pub fn gauge(&self, name: &str) -> IntGauge {
        self.gauges.with_label_values(&[name])
    }

    #[inline]
    pub fn peer_gauge(&self, name: &str, remote_peer_id: &str) -> IntGauge {
        self.peer_gauges.with_label_values(&[name, remote_peer_id])
    }

    #[inline]
    pub fn counter(&self, name: &str) -> IntCounter {
        self.counters.with_label_values(&[name])
    }

    #[inline]
    pub fn histogram(&self, name: &str) -> Histogram {
        self.duration_histograms.with_label_values(&[name])
    }

    pub fn duration_histogram(&self, name: &str) -> DurationHistogram {
        DurationHistogram::new(self.duration_histograms.with_label_values(&[name]))
    }

    #[inline]
    pub fn inc(&self, op: &str) {
        self.counters.with_label_values(&[op]).inc();
    }

    #[inline]
    pub fn inc_by(&self, op: &str, v: usize) {
        // The underlying method is expecting i64, but most of the types
        // we're going to log are `u64` or `usize`.
        self.counters.with_label_values(&[op]).inc_by(v as i64);
    }

    #[inline]
    pub fn add(&self, op: &str) {
        self.gauges.with_label_values(&[op]).inc();
    }

    #[inline]
    pub fn sub(&self, op: &str) {
        self.gauges.with_label_values(&[op]).dec();
    }

    #[inline]
    pub fn set(&self, op: &str, v: usize) {
        // The underlying method is expecting i64, but most of the types
        // we're going to log are `u64` or `usize`.
        self.gauges.with_label_values(&[op]).set(v as i64);
    }

    #[inline]
    pub fn observe(&self, op: &str, v: f64) {
        self.duration_histograms.with_label_values(&[op]).observe(v);
    }

    pub fn observe_duration(&self, op: &str, d: Duration) {
        // Duration is full seconds + nanos elapsed from the presious full second
        let v = d.as_secs() as f64 + f64::from(d.subsec_nanos()) / 1e9;
        self.duration_histograms.with_label_values(&[op]).observe(v);
    }

    pub fn timer(&self, op: &str) -> HistogramTimer {
        self.duration_histograms
            .with_label_values(&[op])
            .start_timer()
    }
}

impl Collector for OpMetrics {
    fn desc(&self) -> Vec<&Desc> {
        let mut ms = Vec::with_capacity(4);
        ms.extend(self.counters.desc());
        ms.extend(self.gauges.desc());
        ms.extend(self.peer_gauges.desc());
        ms.extend(self.duration_histograms.desc());
        ms
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut ms = Vec::with_capacity(4);
        ms.extend(self.counters.collect());
        ms.extend(self.gauges.collect());
        ms.extend(self.peer_gauges.collect());
        ms.extend(self.duration_histograms.collect());
        ms
    }
}

/// Type of the replicated value
/// These are labels for values in a record that is associated with `BvmAddr`
#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub enum ContInfTblValueTag {
    ContactInfo(BvmAddr),
    Vote(BvmAddr),
    EpochSlots(BvmAddr),
}

impl fmt::Display for ContInfTblValueTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContInfTblValueTag::ContactInfo(_) => write!(f, "ContactInfo({})", self.address()),
            ContInfTblValueTag::Vote(_) => write!(f, "Vote({})", self.address()),
            ContInfTblValueTag::EpochSlots(_) => write!(f, "EpochSlots({})", self.address()),
        }
    }
}

impl ContInfTblValueTag {
    pub fn address(&self) -> BvmAddr {
        match self {
            ContInfTblValueTag::ContactInfo(p) => *p,
            ContInfTblValueTag::Vote(p) => *p,
            ContInfTblValueTag::EpochSlots(p) => *p,
        }
    }
}

impl ContInfTblValue {
    /// Totally unsecure unverfiable wallclock of the node that generated this message
    /// Latest wallclock is always picked.
    /// This is used to time out push messages.
    pub fn wallclock(&self) -> u64 {
        match self {
            ContInfTblValue::ContactInfo(contact_info) => contact_info.wallclock,
            ContInfTblValue::Vote(vote) => vote.wallclock,
            ContInfTblValue::EpochSlots(vote) => vote.wallclock,
        }
    }
    pub fn label(&self) -> ContInfTblValueTag {
        match self {
            ContInfTblValue::ContactInfo(contact_info) => {
                ContInfTblValueTag::ContactInfo(contact_info.address())
            }
            ContInfTblValue::Vote(vote) => ContInfTblValueTag::Vote(vote.address()),
            ContInfTblValue::EpochSlots(slots) => ContInfTblValueTag::EpochSlots(slots.address()),
        }
    }
    pub fn contact_info(&self) -> Option<&ContactInfo> {
        match self {
            ContInfTblValue::ContactInfo(contact_info) => Some(contact_info),
            _ => None,
        }
    }
    pub fn vote(&self) -> Option<&Vote> {
        match self {
            ContInfTblValue::Vote(vote) => Some(vote),
            _ => None,
        }
    }
    pub fn epoch_slots(&self) -> Option<&EpochSlots> {
        match self {
            ContInfTblValue::EpochSlots(slots) => Some(slots),
            _ => None,
        }
    }
    /// Return all the possible labels for a record identified by BvmAddr.
    pub fn record_labels(key: &BvmAddr) -> [ContInfTblValueTag; 3] {
        [
            ContInfTblValueTag::ContactInfo(*key),
            ContInfTblValueTag::Vote(*key),
            ContInfTblValueTag::EpochSlots(*key),
        ]
    }
}

impl Signable for ContInfTblValue {
    fn sign(&mut self, keypair: &Keypair) {
        match self {
            ContInfTblValue::ContactInfo(contact_info) => contact_info.sign(keypair),
            ContInfTblValue::Vote(vote) => vote.sign(keypair),
            ContInfTblValue::EpochSlots(epoch_slots) => epoch_slots.sign(keypair),
        };
    }

    fn verify(&self) -> bool {
        match self {
            ContInfTblValue::ContactInfo(contact_info) => contact_info.verify(),
            ContInfTblValue::Vote(vote) => vote.verify(),
            ContInfTblValue::EpochSlots(epoch_slots) => epoch_slots.verify(),
        }
    }

    fn address(&self) -> BvmAddr {
        match self {
            ContInfTblValue::ContactInfo(contact_info) => contact_info.address(),
            ContInfTblValue::Vote(vote) => vote.address(),
            ContInfTblValue::EpochSlots(epoch_slots) => epoch_slots.address(),
        }
    }

    fn signable_data(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn get_signature(&self) -> Signature {
        match self {
            ContInfTblValue::ContactInfo(contact_info) => contact_info.get_signature(),
            ContInfTblValue::Vote(vote) => vote.get_signature(),
            ContInfTblValue::EpochSlots(epoch_slots) => epoch_slots.get_signature(),
        }
    }

    fn set_signature(&mut self, _: Signature) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connection_info::ContactInfo;
    use crate::test_tx::test_tx;
    use bincode::deserialize;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::timing::timestamp;

    #[test]
    fn test_labels() {
        let mut hits = [false; 3];
        // this method should cover all the possible labels
        for v in &ContInfTblValue::record_labels(&BvmAddr::default()) {
            match v {
                ContInfTblValueTag::ContactInfo(_) => hits[0] = true,
                ContInfTblValueTag::Vote(_) => hits[1] = true,
                ContInfTblValueTag::EpochSlots(_) => hits[2] = true,
            }
        }
        assert!(hits.iter().all(|x| *x));
    }
    #[test]
    fn test_keys_and_values() {
        let v = ContInfTblValue::ContactInfo(ContactInfo::default());
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().contact_info().unwrap().id;
        assert_eq!(v.label(), ContInfTblValueTag::ContactInfo(key));

        let v = ContInfTblValue::Vote(Vote::new(&BvmAddr::default(), test_tx(), 0));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().vote().unwrap().from;
        assert_eq!(v.label(), ContInfTblValueTag::Vote(key));

        let v = ContInfTblValue::EpochSlots(EpochSlots::new(BvmAddr::default(), 0, BTreeSet::new(), 0));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().epoch_slots().unwrap().from;
        assert_eq!(v.label(), ContInfTblValueTag::EpochSlots(key));
    }
    #[test]
    fn test_signature() {
        let keypair = Keypair::new();
        let wrong_keypair = Keypair::new();
        let mut v =
            ContInfTblValue::ContactInfo(ContactInfo::new_localhost(&keypair.address(), timestamp()));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
        v = ContInfTblValue::Vote(Vote::new(&keypair.address(), test_tx(), timestamp()));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
        let btreeset: BTreeSet<u64> = vec![1, 2, 3, 6, 8].into_iter().collect();
        v = ContInfTblValue::EpochSlots(EpochSlots::new(keypair.address(), 0, btreeset, timestamp()));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
    }

    fn test_serialize_deserialize_value(value: &mut ContInfTblValue, keypair: &Keypair) {
        let num_tries = 10;
        value.sign(keypair);
        let original_signature = value.get_signature();
        for _ in 0..num_tries {
            let serialized_value = serialize(value).unwrap();
            let deserialized_value: ContInfTblValue = deserialize(&serialized_value).unwrap();

            // Signatures shouldn't change
            let deserialized_signature = deserialized_value.get_signature();
            assert_eq!(original_signature, deserialized_signature);

            // After deserializing, check that the signature is still the same
            assert!(deserialized_value.verify());
        }
    }

    fn verify_signatures(
        value: &mut ContInfTblValue,
        correct_keypair: &Keypair,
        wrong_keypair: &Keypair,
    ) {
        assert!(!value.verify());
        value.sign(&correct_keypair);
        assert!(value.verify());
        value.sign(&wrong_keypair);
        assert!(!value.verify());
        test_serialize_deserialize_value(value, correct_keypair);
    }
}
