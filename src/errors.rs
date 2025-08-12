// REVIEW: I'm not sure we need enum with errors. Meanwhiel we are using standard ones, especially
// since emacs crate is using anyhow.

/// UserFacing tag for anyhow::Error to be outputted as lspce-message
#[derive(Debug)]
pub struct UserFacing;

impl std::error::Error for UserFacing {}

impl std::fmt::Display for UserFacing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UserFacing")
    }
}
