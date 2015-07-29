# Home monitoring system

## Configuration

### Configuring data collection server

Create config file (e.g. `config.json`) that contains configuration of data
collection servers. Take a look at `example_config.json` for an example.

### Configuring web server

Create password hash:

    python -c 'from passlib.hash import sha256_crypt; print(sha256_crypt.encrypt("MYPASSWORD"))' > password

Create (self-signed) certificate for HTTPS:

    openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout server.key -out server.crt

## Running the system

### Running data collection server

    ./monitoring.py config.json

### Creating graphs

    ./create_graphs.py -o static/images/temperatures config.json

### Running web server

    ./server.py
