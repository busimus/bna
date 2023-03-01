const fs = require("fs")
const path = require("path")
const util = require("util")
const { toHexString, hexToUint8Array } = require("idena-sdk-js")

const {
  ContractRunnerProvider,
  ContractArgumentFormat,
} = require("idena-sdk-tests")

// To run tests you need to modify the claim function (because the setIdentity method doesn't work in the simulator)

async function get_provider(airdrop = false) {
  const wasm = path.join(".", "build", "release", "bna_token_erc20.wasm")
  const provider = ContractRunnerProvider.create("http://127.0.0.1:3333", "")
  const code = fs.readFileSync(wasm)

  await provider.Chain.generateBlocks(1)
  await provider.Chain.resetTo(2)

  const deployTx = await provider.Contract.deploy(
    "99999",
    "9999",
    code,
    Buffer.from("")
  )
  await provider.Chain.generateBlocks(1)

  const deployReceipt = await provider.Chain.receipt(deployTx)
  console.log(
    util.inspect(deployReceipt, {
      showHidden: false,
      depth: null,
      colors: true,
    })
  )
  expect(deployReceipt.success).toBe(true)
  if (airdrop) {
    const claimTx = await provider.Contract.call(
      deployReceipt.contract,
      "claimAirdrop",
      "0",
      "1000",
      []
    )

    await provider.Chain.generateBlocks(1)

    const claimReceipt = await provider.Chain.receipt(claimTx)
    console.log(claimReceipt)
    for (var i = 0; i < (claimReceipt.events || []).length; i++) {
      console.log(claimReceipt.events[i])
    }
    for (var i = 0; i < (claimReceipt.actionResult.subActionResults || []).length; i++) {
      console.log(claimReceipt.actionResult.subActionResults[i])
    }
    expect(claimReceipt.success).toBe(true)
    expect(claimReceipt.actionResult.subActionResults[1].success).toBe(true)
  }
  return {provider: provider, contract: deployReceipt.contract}
}


it("can deploy and airdrop", async () => {
  const {provider, contract} = await get_provider(false)
  const godStr = await provider.Chain.godAddress()


  const expectedAirdropAmount = "520000000000000000000"
  const claimTx = await provider.Contract.call(
    contract,
    "claimAirdrop",
    "0",
    "1000",
    []
  )

  await provider.Chain.generateBlocks(1)

  const claimReceipt = await provider.Chain.receipt(claimTx)
  console.log(claimReceipt)
  for (var i = 0; i < (claimReceipt.events || []).length; i++) {
    console.log(claimReceipt.events[i])
  }
  for (var i = 0; i < (claimReceipt.actionResult.subActionResults || []).length; i++) {
    console.log(claimReceipt.actionResult.subActionResults[i])
  }
  expect(claimReceipt.success).toBe(true)
  expect(claimReceipt.actionResult.subActionResults[1].success).toBe(true)

  let airdropAmount = await provider.Contract.readMap(
    contract,
    "b:",
    godStr,
    "bigint"
  )
  expect(airdropAmount).toBe(expectedAirdropAmount)

  let contractState = await provider.Contract.readData(
    contract,
    "STATE",
    "string"
  )
  expect(JSON.parse(contractState).totalSupply).toBe(expectedAirdropAmount)

  const secondClaimTx = await provider.Contract.call(
    contract,
    "claimAirdrop",
    "0",
    "1000",
    []
  )

  await provider.Chain.generateBlocks(1)

  const secondClaimReceipt = await provider.Chain.receipt(secondClaimTx)
  console.log(secondClaimReceipt)
  for (var i = 0; i < (secondClaimReceipt.events || []).length; i++) {
    console.log(secondClaimReceipt.events[i])
  }
  for (var i = 0; i < (secondClaimReceipt.actionResult.subActionResults || []).length; i++) {
    console.log(secondClaimReceipt.actionResult.subActionResults[i])
  }
  expect(secondClaimReceipt.success).toBe(true)
  expect(secondClaimReceipt.actionResult.subActionResults[1].success).toBe(false)

  airdropAmount = await provider.Contract.readMap(
    contract,
    "b:",
    godStr,
    "bigint"
  )
  expect(airdropAmount).toBe(expectedAirdropAmount)
})


it("can transfer", async () => {
  const {provider, contract} = await get_provider(true)
  const godStr = await provider.Chain.godAddress()
  const god = hexToUint8Array(await provider.Chain.godAddress())
  const dest = new Uint8Array(20)
  dest[19] = 1

  let sendTokens = 100

  const transferTx = await provider.Contract.call(
    contract,
    "transfer",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: toHexString(dest),
      },
      {
        index: 1,
        format: ContractArgumentFormat.Bigint,
        value: sendTokens.toString(),
      },
    ]
  )

  await provider.Chain.generateBlocks(1)

  const transferReceipt = await provider.Chain.receipt(transferTx)
  console.log(transferReceipt)
  for (var i = 0; i < (transferReceipt.events || []).length; i++) {
    console.log(transferReceipt.events[i])
  }
  expect(transferReceipt.success).toBe(true)

  destBalance = await provider.Contract.readMap(
    contract,
    "b:",
    toHexString(dest),
    "bigint"
  )

  expect(destBalance).toBe(sendTokens.toString())

  ownerBalance = await provider.Contract.readMap(
    contract,
    "b:",
    godStr,
    "bigint"
  )
  expect(ownerBalance).toBe("519999999999999999900")

  const selfTransferTx = await provider.Contract.call(
    contract,
    "transfer",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: godStr,
      },
      {
        index: 1,
        format: ContractArgumentFormat.Bigint,
        value: sendTokens.toString(),
      },
    ]
  )

  await provider.Chain.generateBlocks(1)

  const selfTransferReceipt = await provider.Chain.receipt(selfTransferTx)
  console.log(selfTransferReceipt)
  for (var i = 0; i < (selfTransferReceipt.events || []).length; i++) {
    console.log(selfTransferReceipt.events[i])
  }
  expect(selfTransferReceipt.success).toBe(true)

  const selfOwnerBalance = await provider.Contract.readMap(
    contract,
    "b:",
    godStr,
    "bigint"
  )
  expect(selfOwnerBalance).toBe(ownerBalance)

  const tooBigTransferTx = await provider.Contract.call(
    contract,
    "transfer",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: godStr,
      },
      {
        index: 1,
        format: ContractArgumentFormat.Bigint,
        value: "9999999999999999999999999",
      },
    ]
  )

  await provider.Chain.generateBlocks(1)

  const tooBigTransferReceipt = await provider.Chain.receipt(tooBigTransferTx)
  console.log(tooBigTransferReceipt)
  for (var i = 0; i < (tooBigTransferReceipt.events || []).length; i++) {
    console.log(tooBigTransferReceipt.events[i])
  }
  expect(tooBigTransferReceipt.success).toBe(false)
})


it("can modify allowance", async () => {
  const {provider, contract} = await get_provider(true)
  const godStr = await provider.Chain.godAddress()
  const god = hexToUint8Array(await provider.Chain.godAddress())
  const dest = new Uint8Array(20)
  dest[19] = 1

  let sendTokens = 100
  const transferTx = await provider.Contract.call(
    contract,
    "transfer",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: toHexString(dest),
      },
      {
        index: 1,
        format: ContractArgumentFormat.Bigint,
        value: sendTokens.toString(),
      },
    ]
  )

  await provider.Chain.generateBlocks(1)

  const transferReceipt = await provider.Chain.receipt(transferTx)
  expect(transferReceipt.success).toBe(true)

  const approveTx = await provider.Contract.call(
    contract,
    "approve",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: toHexString(dest),
      },
      {
        index: 1,
        format: ContractArgumentFormat.Bigint,
        value: sendTokens.toString(),
      },
    ]
  )
  await provider.Chain.generateBlocks(1)

  let receipt = await provider.Chain.receipt(approveTx)
  expect(receipt.success).toBe(true)

  let allowanceTx = await provider.Contract.call(
    contract,
    "allowance",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: godStr,
      },
      {
        index: 1,
        format: ContractArgumentFormat.Hex,
        value: toHexString(dest),
      },

    ]
  )
  await provider.Chain.generateBlocks(1)

  receipt = await provider.Chain.receipt(allowanceTx)
  expect(receipt.success).toBe(true)

  expect(receipt.actionResult.outputData).toBe("0x00000000000000000000000000000064")
  const normalAllowance = parseInt(receipt.actionResult.outputData)

  let utf8Encode = new TextEncoder()
  const key = toHexString(god, false) + ":" + toHexString(dest, false)

  console.log(`key hex=${toHexString(utf8Encode.encode(key))}`)
  let approve = await provider.Contract.readMap(
    contract,
    "a:",
    toHexString(utf8Encode.encode(key)),
    "hex"
  )
  console.log(approve)

  const decreaseAmount = 10
  const decreaseAllowanceTx = await provider.Contract.call(
    contract,
    "decreaseAllowance",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: toHexString(dest),
      },
      {
        index: 1,
        format: ContractArgumentFormat.Bigint,
        value: decreaseAmount.toString(),
      },
    ]
  )
  await provider.Chain.generateBlocks(1)

  let decreaseReceipt = await provider.Chain.receipt(decreaseAllowanceTx)
  expect(decreaseReceipt.success).toBe(true)

  const decreasedAllowanceTx = await provider.Contract.call(
    contract,
    "allowance",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Hex,
        value: godStr,
      },
      {
        index: 1,
        format: ContractArgumentFormat.Hex,
        value: toHexString(dest),
      },

    ]
  )
  await provider.Chain.generateBlocks(1)

  receipt = await provider.Chain.receipt(decreasedAllowanceTx)
  expect(receipt.success).toBe(true)

  const decreasedAllowance = parseInt(receipt.actionResult.outputData)
  expect(decreasedAllowance).toBe(normalAllowance - decreaseAmount)
  // const transferFromTx = await provider.Contract.call(
  //   contract,
  //   "transferFrom",
  //   "0",
  //   "9999",
  //   [
  //     {
  //       index: 0,
  //       format: ContractArgumentFormat.Hex,
  //       value: toHexString(dest),
  //     },
  //     {
  //       index: 1,
  //       format: ContractArgumentFormat.Hex,
  //       value: godStr,
  //     },
  //     {
  //       index: 2,
  //       format: ContractArgumentFormat.Bigint,
  //       value: (10).toString(),
  //     },
  //   ]
  // )

  // await provider.Chain.generateBlocks(1)

  // const transferFromReceipt = await provider.Chain.receipt(transferFromTx)
  // console.log(transferFromReceipt)
  // for (var i = 0; i < (transferFromReceipt.events || []).length; i++) {
  //   console.log(transferFromReceipt.events[i])
  // }
  // expect(transferFromReceipt.success).toBe(true)
})


it("can burn", async () => {
  const {provider, contract} = await get_provider(true)
  const godStr = await provider.Chain.godAddress()

  const burnAmount = 10000000000
  const ownerBalanceBefore = await provider.Contract.readMap(
    contract,
    "b:",
    godStr,
    "bigint"
  )
  const totalSupplyBefore = JSON.parse(await provider.Contract.readData(
    contract,
    "STATE",
    "string"
  )).totalSupply

  const burnTx = await provider.Contract.call(
    contract,
    "burn",
    "0",
    "9999",
    [
      {
        index: 0,
        format: ContractArgumentFormat.Bigint,
        value: burnAmount.toString(),
      },
    ]
  )
  await provider.Chain.generateBlocks(1)

  let burnReceipt = await provider.Chain.receipt(burnTx)
  expect(burnReceipt.success).toBe(true)

  const ownerBalanceAfter = await provider.Contract.readMap(
    contract,
    "b:",
    godStr,
    "bigint"
  )
  const totalSupplyAfter = JSON.parse(await provider.Contract.readData(
    contract,
    "STATE",
    "string"
  )).totalSupply
  expect(ownerBalanceAfter).toBe((ownerBalanceBefore - burnAmount).toString())
  expect(totalSupplyAfter).toBe((totalSupplyBefore - burnAmount).toString())
})
