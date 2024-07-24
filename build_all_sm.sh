# Capture the start time
start_time=$(date +%s)

echo ""
echo "# REMOVING ROOT MODULES AND LOCKS"
echo ""
rm -rf package-lock.json && rm -rf yarn.lock && rm -rf node_modules && rm -rf lib && yarn
echo ""
echo "# INSTALLING drift-common"
echo ""
cd drift-common && rm -rf package-lock.json && rm -rf yarn.lock && rm -rf node_modules && yarn && cd ..
echo ""
echo "# INSTALLING drift-common/protocol/sdk"
echo ""
cd drift-common/protocol/sdk && rm -rf node_modules && yarn && yarn build && yarn link && cd ../../..
echo ""
echo "# INSTALLING drift-common/common-ts"
echo ""
cd drift-common/common-ts && rm -rf package-lock.json && rm -rf node_modules && yarn && yarn link @drift-labs/sdk && yarn build && yarn link && cd ../..
echo ""
echo "# INSTALLING drift-common/icons"
echo ""
cd drift-common/icons && rm -rf package-lock.json && rm -rf yarn.lock && rm -rf node_modules && yarn && yarn build && yarn link && cd ../..
echo ""
echo "# INSTALLING drift-common/react"
echo ""
cd drift-common/react && rm -rf package-lock.json && rm -rf yarn.lock && rm -rf node_modules && yarn && yarn build && yarn link @drift-labs/sdk && yarn link @drift/common && yarn link @drift-labs/icons && yarn link && cd ../..
echo ""
echo "# INSTALLING dlob-server"
echo ""
rm -rf node_modules && yarn && yarn link @drift/common && yarn link @drift-labs/sdk && yarn build
echo ""

# Capture the end time
end_time=$(date +%s)

# Calculate and print the duration
duration=$((end_time - start_time))
echo "FINISHED .. Total time taken: $duration seconds"
