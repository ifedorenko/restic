OneDrive Restic Setup
=====================

Restic requires one time cumbersome manual configuration to access OneDrive repository. At high level this manual cumbersom configuration will:

* Register an application in MS dev portal
* Authorize the new app's access to your personal OneDrive
* Get access and refresh tokens and write them to a json file

Once configured `restic` can access OneDrive completely unattended, like any other supported repository backend.

# Register app in MS dev portal

Folow instructions on [Registering your app for Microsoft Graph](https://docs.microsoft.com/en-us/onedrive/developer/rest-api/getting-started/app-registration) page to register new application and generate application password:

* I don't think app name is significant, pick any.
* Write down application id, this is the `client id` you will need later.
* Generate new password and write it down, this is the `client secret` you will need later.
* Add Web platform, keep `http://localhost` redirect URL.
* Leave other settings as-is and press save.


# OAuth2 code flow

Again, MS documentation is pretty good, follow "code flow" of [OneDrive authentication and sign-in](https://docs.microsoft.com/en-us/onedrive/developer/rest-api/getting-started/msa-oauth#code-flow) instructions.

## Authorize your app to access you OneDrive

Open the following URL in a web browser:

```http
https://login.microsoftonline.com/common/oauth2/v2.0/authorize?client_id={client_id}
  &scope=offline_access%20Files.ReadWrite.All
  &response_type=code
  &redirect_uri=http%3A%2F%2Flocalhost
```

> * the URL is one line without whitestpaces, it's split here for readeability
> * replace `{client_id}` with your registered application id
> * scope grants your application offline (i.e. unattended) read/write access to OneDrive
> * redirect_uri is url-encoded `http://localhost` from app's Web platform config

You may be asked to login to your OneDrive Personal account, then you will need to authorize your app access your OneDrive account. In the end you will be redirected to a dead URL like below

```
http://localhost/?code=df6aa589-1080-b241-b410-c4dff65dbf7c
```

Take a note of the code, i.e. `df6aa589-1080-b241-b410-c4dff65dbf7c` in the example above.

## Redeem the code for access and refresh tokens

This step requires a tool that can send `POST` HTTP requests. I used [Postman](https://www.getpostman.com/), but you can any other tool or write a script.

Issue the following HTTP `POST` request:

```http
POST https://login.microsoftonline.com/common/oauth2/v2.0/token
Content-Type: application/x-www-form-urlencoded

client_id={client_id}&redirect_uri={redirect_uri}&client_secret={client_secret}
&code={code}&grant_type=authorization_code
```

You should get JSON response like this:

```json
{
  "token_type": "Bearer",
  "scope": "Files.ReadWrite.All",
  "expires_in": 3600,
  "ext_expires_in": 0,
  "access_token":"EwCo...AA==",
  "refresh_token":"eyJh...9323"
}
```

## Write `onedrive-secrets.json` file

```json
{
  "ClientID": "",
  "ClientSecret": "",
  "Token": {
    "AccessToken":  "",
    "RefreshToken": "",
    "Expiry": ""
  }
}
```
> * `ClientID` and `ClientSecret` are your aplication id and password, respectively
> * `AccessToken` and `RefreshToken` are access and refresh tokens from OAuth2 POST request above
> * `Expiry` is access token expire time like `2018-02-13T01:46:51.000Z` (I use output of shell `date -u +"%FT%T.000Z"`)


## Running `restic` with OneDrive

Export `RESTIC_ONEDRIVE_SECRETS_FILE` shell variable to point to `onedrive-secrets.json` file:

```shell
# export RESTIC_ONEDRIVE_SECRETS_FILE=/Users/igor/.config/restic/onedrive-secrets.json-backup
```

Use `onedrive:path` restic repository URL to access OneDrive repositories, for example, `onedrive:backups/nas` (note there is no leading `/` in the repository path).

```shell
# restic -r onedrive:restic-test init
```
