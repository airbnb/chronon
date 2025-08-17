#!/bin/bash

# Install Bazel 6.4.0 (required by this project)
echo "Installing Bazel 6.4.0..."

# Add Bazel GPG key and repository
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor | sudo tee /usr/share/keyrings/bazel-archive-keyring.gpg > /dev/null
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list

# Update package list and install specific Bazel version
sudo apt update
sudo apt install -y bazel-6.4.0

# Set up bazel-6.4.0 as the default bazel command
sudo update-alternatives --install /usr/bin/bazel bazel /usr/bin/bazel-6.4.0 100

# Install Thrift compiler (0.13.0 to match project requirements)
echo "Installing Thrift 0.13.0..."
./.devcontainer/install_thrift.sh

# Verify installations
echo "Verifying installations..."
bazel version
echo "Setup complete!"