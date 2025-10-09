#!/bin/bash
echo "Adding edgecase exceptions"
git-secrets --add-provider -- cat scripts/git-secrets/nhsd-rules-deny.txt

echo "Edgecases added"
git-secrets --list
