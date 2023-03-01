import {
  Address,
  Bytes,
  Context,
  Balance,
  PersistentMap,
  util,
  Host,
  models,
} from "idena-sdk-as"
import { Protobuf } from 'as-proto';

const LAST_CLAIM_EPOCH: u16 = 103

export class IRC20 {

  name: string = "BNA"
  symbol: string = "BNA"
  decimals: u16 = 18

  balances: PersistentMap<Address, Balance>
  approves: PersistentMap<string, Balance>
  claims: PersistentMap<Address, Balance>
  totalSupply: Balance

  constructor() {
    this.balances = PersistentMap.withStringPrefix<Address, Balance>("b:");
    this.approves = PersistentMap.withStringPrefix<string, Balance>("a:");
    this.claims = PersistentMap.withStringPrefix<Address, Balance>("c:");
    this.totalSupply = Balance.from(0)
  }

  @view
  getBalance(owner: Address): Balance {
    return this.balances.get(owner, Balance.from(0))
  }

  transfer(to: Address, amount: Balance): void {
    let sender = Context.caller()

    const fromAmount = this.getBalance(sender)
    util.assert(fromAmount >= amount, "Transfer amount exceeds balance")
    this.balances.set(sender, fromAmount - amount)
    let destBalance = this.getBalance(to)
    this.balances.set(to, destBalance + amount)
    Host.emitEvent("Transfer", [sender, to, Bytes.fromBytes(amount.toBytes())])
  }

  approve(spender: Address, amount: Balance): void {
    let sender = Context.caller()
    this.approves.set(sender.toHex() + ":" + spender.toHex(), amount)
    Host.emitEvent("Approval", [sender, spender, Bytes.fromBytes(amount.toBytes())])
  }

  decreaseAllowance(spender: Address, subtractedValue: Balance): void {
    let sender = Context.caller()
    let currentAllowance = this.allowance(sender, spender);
    util.assert(currentAllowance >= subtractedValue, "Decreased allowance below zero");
    this.approve(spender, currentAllowance - subtractedValue);
  }

  @view
  allowance(tokenOwner: Address, spender: Address): Balance {
    const key = tokenOwner.toHex() + ":" + spender.toHex()
    return this.approves.get(key, Balance.from(0))
  }

  transferFrom(from: Address, to: Address, amount: Balance): void {
    let caller = Context.caller()

    const fromAmount = this.getBalance(from)
    util.assert(fromAmount >= amount, "Transfer amount exceeds balance")
    const approvedAmount = this.allowance(from, caller)
    util.assert(approvedAmount >= amount, "Insufficient allowance")

    this.balances.set(from, fromAmount - amount)
    let destBalance = this.getBalance(to)
    this.balances.set(to, destBalance + amount)
    Host.emitEvent("Transfer", [from, to, Bytes.fromBytes(amount.toBytes())])
  }

  claimAirdrop(): void {
    let receiver = Context.caller()
    Host.emitEvent("ClaimedAmount", [receiver, Bytes.fromBytes(this.claims.get(receiver, Balance.Zero).toBytes())])
    Host.createGetIdentityPromise(receiver, 200000).then("_claimForIdentity", [], Balance.Zero, 5000000)
  }

  @mutateState
  _claimForIdentity(): void {
    const receiver = Context.originalCaller()
    util.assert(this.claims.get(receiver, Balance.Zero) == Balance.Zero, "Already claimed")
    util.assert(Context.epoch() <= LAST_CLAIM_EPOCH, "Claim period is over")

    let identData = Host.promiseResult().value()
    Host.emitEvent("IdentityData", [identData])
    util.assert(Host.promiseResult().failed() === false, 'Failed to get identity')

    const identity = Protobuf.decode<models.ProtoStateIdentity>(identData, models.ProtoStateIdentity.decode);
    const identAge = Context.epoch() - identity.birthday
    const airdrop = this.getAirdropAmount(identity.state, identAge)
    // const airdrop = this.getAirdropAmount(8, 52)  // for testing since changing identity age/state doesn't work in the simulator

    this.balances.set(receiver, this.getBalance(receiver) + airdrop)
    this.claims.set(receiver, airdrop)
    this.totalSupply = this.totalSupply + airdrop
    Host.emitEvent("Airdrop", [receiver, Bytes.fromBytes(airdrop.toBytes())])
  }

  getAirdropAmount(identityState: u32, identityAge: u32): Balance {
    let airdrop: Balance;
    if (identityState == 4 || identityState == 6) { // Suspended, Zombie
      airdrop = Balance.fromString('200000000000000000')
    } else if (identityState == 3 || identityState == 7 || identityState == 8) { // Verified, Newbie, Human
      airdrop = Balance.fromString('1000000000000000000')
    } else {
      util.assert(false, "Not validated identity")
    }

    let multiplier = Balance.from(identityAge)
    multiplier = multiplier * Balance.max(multiplier / Balance.from(10), Balance.from(1)) * Balance.from(2)
    airdrop = airdrop * multiplier
    return airdrop
  }

  @mutateState
  burn(amount: Balance): void {
    let sender = Context.caller()
    const fromAmount = this.getBalance(sender)
    util.assert(fromAmount >= amount, "Burn amount exceeds balance")
    this.balances.set(sender, fromAmount - amount)
    this.totalSupply = this.totalSupply - amount
  }
}
