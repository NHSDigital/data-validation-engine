#!/bin/bash
echo "[1/4] Cloning Git Secrets"
installPath="$HOME/git-secrets-temp"
if [ -d "$installPath" ];
then
  echo "Git secrets already cloned"
else
  git clone https://github.com/awslabs/git-secrets.git $installPath
fi

echo "" && echo "[2/4] Installing Git Secrets and Adding to PATH"
pushd $installPath
make install PREFIX="$HOME/git-secrets"
echo 'export PATH="$HOME/git-secrets/bin":$PATH' >> ~/.bashrc
source ~/.bashrc
popd

echo "" && echo "[3/4] Adding Git Hooks"
$HOME/git-secrets/bin/git-secrets --install -f

echo "" && echo "[4/4] Removing Temp Git Secrets Repo"
rm -rf $installPath
