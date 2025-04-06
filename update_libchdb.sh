#!/bin/bash

set -e

# Check for necessary tools
command -v curl >/dev/null 2>&1 || { echo >&2 "curl is required but it's not installed. Aborting."; exit 1; }
command -v tar >/dev/null 2>&1 || { echo >&2 "tar is required but it's not installed. Aborting."; exit 1; }

# Function to download and extract the file
download_and_extract() {
    local url=$1
    local file="libchdb.tar.gz"

    echo "Attempting to download $PLATFORM from $url"

    # Download the file with a retry logic
    if curl -L -o "$file" "$url"; then
        echo "Download successful."

        # Optional: Verify download integrity here, if checksums are provided

        # Untar the file
        if tar -xzf "$file"; then
            echo "Extraction successful."
            return 0
        fi
    fi
    return 1
}

# Get the newest release version
LATEST_RELEASE=v2.1.1

# Select the correct package based on OS and architecture
case "$(uname -s)" in
    Linux)
        if [[ $(uname -m) == "aarch64" ]]; then
            PLATFORM="linux-aarch64-libchdb.tar.gz"
        else
            PLATFORM="linux-x86_64-libchdb.tar.gz"
        fi
        ;;
    Darwin)
        if [[ $(uname -m) == "arm64" ]]; then
            PLATFORM="macos-arm64-libchdb.tar.gz"
        else
            PLATFORM="macos-x86_64-libchdb.tar.gz"
        fi
        ;;
    *)
        echo "Unsupported platform"
        exit 1
        ;;
esac

# Main download URL
DOWNLOAD_URL="https://github.com/chdb-io/chdb/releases/download/$LATEST_RELEASE/$PLATFORM"
FALLBACK_URL="https://github.com/chdb-io/chdb/releases/latest/download/$PLATFORM"

# Try the main download URL first
if ! download_and_extract "$DOWNLOAD_URL"; then
    echo "Retrying with fallback URL..."
    if ! download_and_extract "$FALLBACK_URL"; then
        echo "Both primary and fallback downloads failed. Aborting."
        exit 1
    fi
fi

# check if --local flag is passed
if [[ "$1" == "--local" ]]; then
    # Set execute permission for libchdb.so
    chmod +x libchdb.so

    # Clean up
    rm -f libchdb.tar.gz
    exit 0
elif [[ "$1" == "--global" ]]; then
  # If current uid is not 0, check if sudo is available and request the user to input the password
  if [[ $EUID -ne 0 ]]; then
      command -v sudo >/dev/null 2>&1 || { echo >&2 "This script requires sudo privileges but sudo is not installed. Aborting."; exit 1; }
      echo "Installation requires administrative access. You will be prompted for your password."
  fi

  # Define color messages if terminal supports them
  if [[ -t 1 ]]; then
      RED='\033[0;31m'
      GREEN='\033[0;32m'
      NC='\033[0m'  # No Color
      REDECHO() { echo -e "${RED}$@${NC}"; }
      GREENECHO() { echo -e "${GREEN}$@${NC}"; }
      ENDECHO() { echo -ne "${NC}"; }
  else
      REDECHO() { echo "$@"; }
      GREENECHO() { echo "$@"; }
      ENDECHO() { :; }
  fi

  # Use sudo if not running as root
  SUDO=''
  if [[ $EUID -ne 0 ]]; then
      SUDO='sudo'
      GREENECHO "\nYou will be asked for your sudo password to install:"
      echo "    libchdb.so to /usr/local/lib/"
      echo "    chdb.h to /usr/local/include/"
  fi

  # Make sure the library and header directory exists
  ${SUDO} mkdir -p /usr/local/lib /usr/local/include || true

  # Install the library and header file
  ${SUDO} /bin/cp libchdb.so /usr/local/lib/
  ${SUDO} /bin/cp chdb.h /usr/local/include/

  # Set execute permission for libchdb.so
  ${SUDO} chmod +x /usr/local/lib/libchdb.so

  # Update library cache (Linux specific)
  if [[ "$(uname -s)" == "Linux" ]]; then
      ${SUDO} ldconfig
  fi

  # Clean up
  rm -f libchdb.tar.gz libchdb.so chdb.h

  GREENECHO "Installation completed successfully." ; ENDECHO
  GREENECHO "If any error occurred, please report it to:" ; ENDECHO
  GREENECHO "    https://github.com/chdb-io/chdb/issues/new/choose" ; ENDECHO
else
  echo "Invalid option. Use --local to install locally or --global to install globally."
  exit 1
fi
