use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Registry;

use crate::QidManager;

pub struct Span;

mod private {
    pub trait Sealed {}
}

pub trait QidMetadataGetter: private::Sealed {
    fn get_qid<Q>(&self) -> Option<Q>
    where
        Q: QidManager;
}

pub trait QidMetadataSetter: private::Sealed {
    fn set_qid<Q>(&mut self, qid: &Q)
    where
        Q: QidManager;
}

impl QidMetadataGetter for Span {
    fn get_qid<Q>(&self) -> Option<Q>
    where
        Q: QidManager,
    {
        tracing::dispatcher::get_default(|dispatch| {
            let registry = dispatch
                .downcast_ref::<Registry>()
                .expect("no global default dispatcher found");
            dispatch.current_span().into_inner().and_then(|(id, _)| {
                let span = registry.span(&id).unwrap();
                let ext = span.extensions();
                ext.get::<Q>().cloned()
            })
        })
    }
}

impl QidMetadataSetter for Span {
    fn set_qid<Q>(&mut self, qid: &Q)
    where
        Q: QidManager,
    {
        tracing::dispatcher::get_default(|dispatch| {
            let registry = dispatch
                .downcast_ref::<Registry>()
                .expect("no global default dispatcher found");
            if let Some((id, _meta)) = dispatch.current_span().into_inner() {
                let span = registry.span(&id).unwrap();
                let mut ext = span.extensions_mut();
                ext.replace(qid.clone());
            }
        });
    }
}

impl private::Sealed for Span {}

impl QidMetadataGetter for tracing::Span {
    fn get_qid<Q>(&self) -> Option<Q>
    where
        Q: QidManager,
    {
        tracing::dispatcher::get_default(|dispatch| {
            let registry = dispatch
                .downcast_ref::<Registry>()
                .expect("no global default dispatcher found");
            self.id().and_then(|id| {
                let span = registry.span(&id).unwrap();
                let ext = span.extensions();
                ext.get::<Q>().cloned()
            })
        })
    }
}

impl QidMetadataSetter for tracing::Span {
    fn set_qid<Q>(&mut self, qid: &Q)
    where
        Q: QidManager,
    {
        tracing::dispatcher::get_default(|dispatch| {
            let registry = dispatch
                .downcast_ref::<Registry>()
                .expect("no global default dispatcher found");
            if let Some(id) = self.id() {
                let span = registry.span(&id).unwrap();
                let mut ext = span.extensions_mut();
                ext.replace(qid.clone());
            }
        });
    }
}

impl private::Sealed for tracing::Span {}

#[cfg(test)]
mod tests {
    use tracing::info_span;

    use super::*;
    use crate::fake::Qid;

    #[test]
    fn qid_set_get_test() {
        let qid_u64 = 9223372036854775807;
        let qid = Qid::from(qid_u64);

        {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            let _guard = tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .set_default();

            tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
                Span.set_qid(&qid);
                let qid: Qid = Span.get_qid().unwrap();
                assert_eq!(qid.get(), qid_u64);
            });
        }

        {
            use tracing_subscriber::layer::SubscriberExt;
            use tracing_subscriber::util::SubscriberInitExt;
            let _guard = tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer())
                .set_default();

            let mut span = info_span!("example");
            span.set_qid(&qid);
            let qid: Qid = span.get_qid().unwrap();
            assert_eq!(qid.get(), qid_u64);
        }
    }

    #[test]
    fn qid_display() {
        assert_eq!(
            format!("{}", Qid::from(0x7fffffffffffffff).display()),
            "0x7fffffffffffffff"
        )
    }
}
