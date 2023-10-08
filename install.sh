#!/usr/bin/bash
source "parameters.py"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo "Script directory: $SCRIPT_DIR"

# check if have permissions
if [ "$EUID" -ne 0 ]
  then echo "Please run as root"
  exit
fi

# check if argument is passed
if [ -z "$api_host" ]; then
    echo "I can't find parameters.py variables. Did you make a copy of example file?"
    echo "i.e.: cp parameters.py.example parameters.py"
    exit 1
fi

# parse the json file
SERVICE_NAME1="FinancasBrasil-FastAPI"
SERVICE_NAME2="FinancasBrasil-Plombery"
DESCRIPTION="Runs an API and Spiders to crawl and serve Brazilian Finance Data "
PKG_PATH="$SCRIPT_DIR/.venv/bin/python"
SERVICE_PATH1="$SCRIPT_DIR/finance-api.py"
SERVICE_PATH2="$SCRIPT_DIR/finance-plombery.py"

if [[ "$1" == "remove" ]]; then
    echo "Uninstalling"
    echo "Stopping services"
    sudo systemctl stop ${SERVICE_NAME1//'.service'/}
    sudo systemctl stop ${SERVICE_NAME2//'.service'/}
    sudo systemctl disable ${SERVICE_NAME1//'.service'/}
    sudo systemctl disable ${SERVICE_NAME2//'.service'/}
    echo "Removing services files"
    sudo rm -rf /etc/systemd/system/${SERVICE_NAME2//'"'/}.service    
    sudo rm -rf /etc/systemd/system/${SERVICE_NAME1//'"'/}.service
    sudo systemctl daemon-reload 
    exit
fi

echo "Creating venv environment"
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt


# check if service is active
IS_ACTIVE=$(sudo systemctl is-active $SERVICE_NAME1)
if [ "$IS_ACTIVE" == "active" ]; then
    # restart the service
    echo "$SERVICE_NAME1 is running"
    echo "Stopping service"
    sudo systemctl stop $SERVICE_NAME1
    echo "Service stop"
fi
# create service file
echo "Creating service file"
sudo cat > /etc/systemd/system/${SERVICE_NAME1//'"'/}.service << EOF
[Unit]
Description=$DESCRIPTION
After=network.target

[Service]
ExecStart=$PKG_PATH $SERVICE_PATH1
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
# restart daemon, enable and start service
echo "Reloading daemon and enabling service"
sudo systemctl daemon-reload 
sudo systemctl enable ${SERVICE_NAME1//'.service'/} # remove the extension
sudo systemctl start ${SERVICE_NAME1//'.service'/}
echo "Service Started"

# check if service is active
IS_ACTIVE=$(sudo systemctl is-active $SERVICE_NAME2)
if [ "$IS_ACTIVE" == "active" ]; then
    # restart the service
    echo "$SERVICE_NAME2 is running"
    echo "Stopping service"
    sudo systemctl stop $SERVICE_NAME2
    echo "Service stop"
fi
# create service file
echo "Creating service file"
sudo cat > /etc/systemd/system/${SERVICE_NAME2//'"'/}.service << EOF
[Unit]
Description=$DESCRIPTION
After=network.target

[Service]
ExecStart=$PKG_PATH $SERVICE_PATH2
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
# restart daemon, enable and start service
echo "Reloading daemon and enabling service"
sudo systemctl daemon-reload 
sudo systemctl enable ${SERVICE_NAME2//'.service'/} # remove the extension
sudo systemctl start ${SERVICE_NAME2//'.service'/}
echo "Service Started"

exit 0