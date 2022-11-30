import fs from "fs";
import { Keypair } from "@solana/web3.js";
import { bs58 } from "@project-serum/anchor/dist/cjs/utils/bytes";
import { Wallet } from "@drift-labs/sdk";

import { logger } from "./logger";

export function getWallet(): Wallet {
  const privateKey = process.env.ANCHOR_PRIVATE_KEY;
  if (!privateKey) {
    throw new Error(
      "Must set environment variable ANCHOR_PRIVATE_KEY with the path to a id.json or a list of commma separated numbers"
    );
  }
  // try to load privateKey as a filepath
  let loadedKey: Uint8Array;
  if (fs.existsSync(privateKey)) {
    logger.info(`loading private key from ${privateKey}`);
    loadedKey = new Uint8Array(
      JSON.parse(fs.readFileSync(privateKey).toString())
    );
  } else {
    if (privateKey.includes(",")) {
      logger.info(`Trying to load private key as comma separated numbers`);
      loadedKey = Uint8Array.from(
        privateKey.split(",").map((val) => Number(val))
      );
    } else {
      logger.info(`Trying to load private key as base58 string`);
      loadedKey = new Uint8Array(bs58.decode(privateKey));
    }
  }

  const keypair = Keypair.fromSecretKey(Uint8Array.from(loadedKey));
  return new Wallet(keypair);
}
