use std::collections::BTreeMap;

use crate::common::StatementCache;
use crate::connection::{sasl, stream::PgStream};
use crate::error::Error;
use crate::io::{StatementId, StatementIdManager};
use crate::message::{
    Authentication, BackendKeyData, BackendMessageFormat, Password, ReadyForQuery, Startup,
};
use crate::{PgConnectOptions, PgConnection};

use super::type_cache::TypeCache;
use super::worker::{WaitType, Worker};
use super::PgConnectionInner;

// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.3
// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.11

impl PgConnection {
    pub(crate) async fn establish(options: &PgConnectOptions) -> Result<Self, Error> {
        // Upgrade to TLS if we were asked to and the server supports it
        let mut stream = PgStream::connect(options).await?;

        let stream_bg = PgStream::connect(options).await?;

        let chan = Worker::spawn(stream_bg);

        // To begin a session, a frontend opens a connection to the server
        // and sends a startup message.

        let mut params = vec![
            // Sets the display format for date and time values,
            // as well as the rules for interpreting ambiguous date input values.
            ("DateStyle", "ISO, MDY"),
            // Sets the client-side encoding (character set).
            // <https://www.postgresql.org/docs/devel/multibyte.html#MULTIBYTE-CHARSET-SUPPORTED>
            ("client_encoding", "UTF8"),
            // Sets the time zone for displaying and interpreting time stamps.
            ("TimeZone", "UTC"),
        ];

        if let Some(ref extra_float_digits) = options.extra_float_digits {
            params.push(("extra_float_digits", extra_float_digits));
        }

        if let Some(ref application_name) = options.application_name {
            params.push(("application_name", application_name));
        }

        if let Some(ref options) = options.options {
            params.push(("options", options));
        }

        let mut manager = chan.manager();

        manager.send_message(|message| {
            message.write(Startup {
                username: Some(&options.username),
                database: options.database.as_deref(),
                params: &params,
            })?;
            Ok(WaitType::NumMessages { num_responses: 1 })
        })?;

        // The server then uses this information and the contents of
        // its configuration files (such as pg_hba.conf) to determine whether the connection is
        // provisionally acceptable, and what additional
        // authentication is required (if any).

        let mut process_id = 0;
        let mut secret_key = 0;
        let transaction_status;

        loop {
            let message = manager.recv().await?;
            match message.format {
                BackendMessageFormat::Authentication => match message.decode()? {
                    Authentication::Ok => {
                        // the authentication exchange is successfully completed
                        // do nothing; no more information is required to continue
                    }

                    Authentication::CleartextPassword => {
                        // The frontend must now send a [PasswordMessage] containing the
                        // password in clear-text form.

                        // stream
                        //     .send(Password::Cleartext(
                        //         options.password.as_deref().unwrap_or_default(),
                        //     ))
                        //     .await?;
                        manager.send_message(|message| {
                            message.write_msg(Password::Cleartext(
                                options.password.as_deref().unwrap_or_default(),
                            ))?;
                            Ok(WaitType::NumMessages { num_responses: 1 })
                        })?;
                    }
                    Authentication::Md5Password(body) => {
                        // The frontend must now send a [PasswordMessage] containing the
                        // password (with user name) encrypted via MD5, then encrypted again
                        // using the 4-byte random salt specified in the
                        // [AuthenticationMD5Password] message.
                        manager.send_message(|message| {
                            message.write_msg(Password::Md5 {
                                username: &options.username,
                                password: options.password.as_deref().unwrap_or_default(),
                                salt: body.salt,
                            })?;

                            Ok(WaitType::NumMessages { num_responses: 1 })
                        })?;
                        // stream
                        //     .send(Password::Md5 {
                        //         username: &options.username,
                        //         password: options.password.as_deref().unwrap_or_default(),
                        //         salt: body.salt,
                        //     })
                        //     .await?;
                    }

                    Authentication::Sasl(body) => {
                        sasl::authenticate(&mut manager, &mut stream, options, body).await?;
                    }

                    method => {
                        return Err(err_protocol!(
                            "unsupported authentication method: {:?}",
                            method
                        ));
                    }
                },

                BackendMessageFormat::BackendKeyData => {
                    // provides secret-key data that the frontend must save if it wants to be
                    // able to issue cancel requests later

                    let data: BackendKeyData = message.decode()?;

                    process_id = data.process_id;
                    secret_key = data.secret_key;
                }

                BackendMessageFormat::ReadyForQuery => {
                    // start-up is completed. The frontend can now issue commands
                    transaction_status = message.decode::<ReadyForQuery>()?.transaction_status;
                    println!("Waited for rfq");

                    break;
                }

                _ => {
                    return Err(err_protocol!(
                        "establish: unexpected message: {:?}",
                        message.format
                    ))
                }
            }
        }

        Ok(PgConnection {
            inner: Box::new(PgConnectionInner {
                chan,
                parameter_statuses: BTreeMap::new(),
                server_version_num: None,
                stream,
                process_id,
                secret_key,
                transaction_status,
                transaction_depth: 0,
                pending_ready_for_query_count: 0,
                stmt_id_manager: StatementIdManager::new(StatementId::NAMED_START),
                cache_statement: StatementCache::new(options.statement_cache_capacity),
                type_cache: TypeCache::new(),
                log_settings: options.log_settings.clone(),
            }),
        })
    }
}
