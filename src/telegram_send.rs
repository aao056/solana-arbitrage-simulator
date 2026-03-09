use reqwest::Client;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize)]
struct SendMsg {
    chat_id: String,
    text: String,
    parse_mode: String,
    disable_web_page_preview: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_thread_id: Option<i64>,
}

// #[derive(Serialize)]
// struct EditMsg {
//     chat_id: String,
//     message_id: i64,
//     text: String,
//     parse_mode: String,
//     disable_web_page_preview: bool,
// }

#[derive(Deserialize)]
struct TgResp<T> {
    result: T,
}

#[derive(Deserialize)]
struct TgMessage {
    message_id: i64,
}

pub async fn tg_send(token: &str, chat_id: &str, text: &str) -> anyhow::Result<i64> {
    tg_send_with_thread(token, chat_id, text, default_thread_id_from_env()).await
}

pub async fn tg_send_with_thread(
    token: &str,
    chat_id: &str,
    text: &str,
    message_thread_id: Option<i64>,
) -> anyhow::Result<i64> {
    let url = format!("https://api.telegram.org/bot{}/sendMessage", token);

    let body = SendMsg {
        chat_id: chat_id.to_string(),
        text: text.to_string(),
        parse_mode: "HTML".to_string(),
        disable_web_page_preview: true,
        message_thread_id,
    };

    let resp: TgResp<TgMessage> = Client::new()
        .post(url)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(resp.result.message_id)
}

fn default_thread_id_from_env() -> Option<i64> {
    std::env::var("TG_MESSAGE_THREAD_ID")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
}

// pub async fn tg_edit(
//     token: &str,
//     chat_id: &str,
//     message_id: i64,
//     text: &str,
// ) -> anyhow::Result<()> {
//     let url = format!("https://api.telegram.org/bot{}/editMessageText", token);

//     let body = EditMsg {
//         chat_id: chat_id.to_string(),
//         message_id,
//         text: text.to_string(),
//         parse_mode: "HTML".to_string(),
//         disable_web_page_preview: true,
//     };

//     Client::new()
//         .post(url)
//         .json(&body)
//         .send()
//         .await?
//         .error_for_status()?;

//     Ok(())
// }
