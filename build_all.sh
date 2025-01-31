git submodule init
git submodule update --recursive

echo "building sdk..."
cd drift-common/protocol/sdk
yarn clean && yarn && yarn build
cd ../../..

echo "building drift-common..."
cd drift-common/common-ts
yarn clean && yarn && yarn build
cd ../..

echo "building dlob server..."
yarn clean && yarn && yarn build
