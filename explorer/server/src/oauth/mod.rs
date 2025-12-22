pub mod client;
pub mod config;
pub mod custom_client;
pub mod handlers;
pub mod middleware;
pub mod plain_client;
pub mod session;

pub use client::OAuthClientEnum;
pub use config::OAuthConfig;
pub use session::SessionManager;

#[cfg(test)]
mod test_documentation {
    //! # OAuth Module Test Coverage
    //!
    //! This module contains comprehensive test coverage for the OAuth2/OIDC authentication system.
    //!
    //! ## Test Files
    //!
    //! ### config.rs (83 tests)
    //! Tests for OAuth configuration structures and validation:
    //! - Default configurations for all OAuth types (OIDC, Plain, Custom)
    //! - Configuration validation (missing fields, invalid URLs, etc.)
    //! - Environment variable loading and parsing
    //! - Provider-specific validation rules
    //! - Serialization/deserialization of configs
    //! - User mapping configurations
    //! - Provider display names (i18n support)
    //! - Fallback from plain to custom config
    //!
    //! Key test categories:
    //! - Default value tests (7 tests)
    //! - OIDC validation tests (6 tests)
    //! - Plain OAuth validation tests (5 tests)
    //! - Custom OAuth validation tests (6 tests)
    //! - Environment variable tests (8 tests)
    //! - Serialization tests (3 tests)
    //! - Helper function tests (2 tests)
    //! - Clone and debug tests (4 tests)
    //!
    //! ### session.rs (46 tests)
    //! Tests for session management and TsDB synchronization:
    //! - TsdbSyncPassword generation (random and constant)
    //! - TsdbSyncUsername generation (default, constant, pattern, usermap)
    //! - Username length limits (23 character TsDB limit)
    //! - Provider abbreviations (oidc -> i, custom -> c)
    //! - Password character validation
    //! - Pattern placeholders ({provider}, {user_id}, {uuid}, {suffix})
    //! - Usermap with fallback behavior
    //! - TsdbSyncOptions integration
    //! - Serialization/deserialization
    //!
    //! Key test categories:
    //! - Password generation tests (8 tests)
    //! - Username generation tests (12 tests)
    //! - Sync options tests (6 tests)
    //! - Serialization tests (5 tests)
    //! - Edge cases (unicode, empty strings, long IDs)
    //!
    //! ### client.rs (25 tests)
    //! Tests for OIDC client and user info structures:
    //! - UserInfo creation and optional fields
    //! - Authorization request structure
    //! - User claims extraction (trait implementation verification)
    //! - Multiple roles handling
    //! - Email format variations
    //! - Unicode username and name support
    //! - Serialization/deserialization
    //! - Debug formatting
    //!
    //! Key test categories:
    //! - UserInfo tests (18 tests)
    //! - AuthorizationRequest tests (4 tests)
    //! - Edge cases (special chars, unicode, empty fields)
    //!
    //! ### plain_client.rs (34 tests)
    //! Tests for standard OAuth 2.0 client:
    //! - Client initialization and validation
    //! - Authorization URL generation
    //! - CSRF token generation (32 alphanumeric characters)
    //! - URL encoding for special characters
    //! - Token response parsing (JSON and query string formats)
    //! - Profile response handling
    //! - Fallback to custom config
    //! - Additional fields in responses
    //! - Unicode support
    //!
    //! Key test categories:
    //! - Client creation tests (4 tests)
    //! - Authorization URL tests (6 tests)
    //! - Token response tests (7 tests)
    //! - Profile response tests (8 tests)
    //! - Edge cases (unicode, empty values, special chars)
    //!
    //! ### custom_client.rs (43 tests)
    //! Tests for custom OAuth provider (TsDB specific):
    //! - Client initialization
    //! - Token response formats (direct and wrapped)
    //! - Profile response with custom attributes
    //! - User synchronization structures
    //! - Login flow responses
    //! - Error handling
    //! - FetchUsersCredentials
    //! - Role and organization structures
    //! - Token trimming
    //!
    //! Key test categories:
    //! - Authorization tests (3 tests)
    //! - Token response tests (7 tests)
    //! - Profile response tests (6 tests)
    //! - Sync user structures (8 tests)
    //! - Login response tests (4 tests)
    //! - Client creation tests (3 tests)
    //! - Custom structures (6 tests)
    //! - Edge cases (unicode, empty arrays, zero expires)
    //!
    //! ## Total Test Count: 231 tests
    //!
    //! ## Test Coverage Areas
    //!
    //! 1. **Configuration Management**
    //!    - All OAuth provider types (OIDC, Plain, Custom)
    //!    - Validation rules and error messages
    //!    - Environment variable overrides
    //!    - Default values and fallbacks
    //!
    //! 2. **Session & User Management**
    //!    - Password generation strategies
    //!    - Username generation patterns
    //!    - TsDB integration constraints
    //!    - User mapping configurations
    //!
    //! 3. **OAuth Flows**
    //!    - Authorization URL generation
    //!    - Token exchange
    //!    - Profile fetching
    //!    - Token refresh
    //!    - CSRF/state protection
    //!
    //! 4. **Data Serialization**
    //!    - JSON serialization/deserialization
    //!    - Query parameter parsing
    //!    - Flexible response formats
    //!    - Additional/custom fields handling
    //!
    //! 5. **Edge Cases & Security**
    //!    - Unicode support
    //!    - URL encoding
    //!    - Empty/null values
    //!    - Token trimming
    //!    - Field validation
    //!
    //! ## Running Tests
    //!
    //! Run all OAuth tests:
    //! ```bash
    //! cargo test -p explorer-server --lib oauth
    //! ```
    //!
    //! Run specific test file:
    //! ```bash
    //! cargo test -p explorer-server --lib oauth::config
    //! cargo test -p explorer-server --lib oauth::session
    //! cargo test -p explorer-server --lib oauth::client
    //! cargo test -p explorer-server --lib oauth::plain_client
    //! cargo test -p explorer-server --lib oauth::custom_client
    //! ```
    //!
    //! Run with output:
    //! ```bash
    //! cargo test -p explorer-server --lib oauth -- --nocapture
    //! ```
}
