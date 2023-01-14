from decimal import Decimal
from bna.transfer import Transfer, BscLog
from bna.bsc_listener import BscListener

logs = [([
    {
      "address": "0x0de08c1abe5fb86dd7fd2ac90400ace305138d5b",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x00000000000000000000000009784d03b42581cfc4fc90a7ab11c3125dedeb86",
        "0x00000000000000000000000005ad60d9a2f1aa30ba0cdbaf1e0a0a145fbea16f"
      ],
      "data": "0x00000000000000000000000000000000000000000000036719bb6db7cebe201c",
      "blockNumber": "0x1344e0d",
      "transactionHash": "0x2b372adda942c2e852ddd7f621f126020e3d59e05aa42a74119ef183a4574152",
      "transactionIndex": "0x2",
      "blockHash": "0x883ab7427555a4f8692213d85e5ac9c5e24f8e192f243493bb069cbd858b78e5",
      "logIndex": "0x5",
      "removed": False
    },
    {
      "address": "0x0de08c1abe5fb86dd7fd2ac90400ace305138d5b",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x00000000000000000000000005ad60d9a2f1aa30ba0cdbaf1e0a0a145fbea16f",
        "0x000000000000000000000000c1bcdc9eb37d8e72ff0e0ca4bc8d19735b1b38ce"
      ],
      "data": "0x0000000000000000000000000000000000000000000001b38cddb6dbe75f100e",
      "blockNumber": "0x1344e0d",
      "transactionHash": "0x2b372adda942c2e852ddd7f621f126020e3d59e05aa42a74119ef183a4574152",
      "transactionIndex": "0x2",
      "blockHash": "0x883ab7427555a4f8692213d85e5ac9c5e24f8e192f243493bb069cbd858b78e5",
      "logIndex": "0x7",
      "removed": False
    },
    {
      "address": "0x0de08c1abe5fb86dd7fd2ac90400ace305138d5b",
      "topics": [
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x00000000000000000000000005ad60d9a2f1aa30ba0cdbaf1e0a0a145fbea16f",
        "0x000000000000000000000000aa4dce8585528265c6bac502ca9578343f82630f"
      ],
      "data": "0x0000000000000000000000000000000000000000000001b38cddb6dbe75f100e",
      "blockNumber": "0x1344e0d",
      "transactionHash": "0x2b372adda942c2e852ddd7f621f126020e3d59e05aa42a74119ef183a4574152",
      "transactionIndex": "0x2",
      "blockHash": "0x883ab7427555a4f8692213d85e5ac9c5e24f8e192f243493bb069cbd858b78e5",
      "logIndex": "0xb",
      "removed": False
    }],
    Transfer(changes={'0x09784d03b42581cfc4fc90a7ab11c3125dedeb86': Decimal('-16068.968284508827557916'),
                  '0xaa4dce8585528265c6bac502ca9578343f82630f': Decimal('8034.484142254413778958'),
                  '0xc1bcdc9eb37d8e72ff0e0ca4bc8d19735b1b38ce': Decimal('8034.484142254413778958')},
         hash='0x2b372adda942c2e852ddd7f621f126020e3d59e05aa42a74119ef183a4574152',
         blockNumber='0x1344e0d',
         logIndex='0x5',
         timeStamp=None,
         chain='bsc',
         signer='0x0000000000000000000000000000000000000000',
         tags=[],
         meta={}))
]

def test_squash():
    for case in logs:
      blogs = list(map(lambda l: BscLog(**l), case[0]))
      tfs = list(map(lambda bl: Transfer.from_bsc_log(bl), blogs))
      sq = BscListener.squash(tfs)
      assert sq == case[1]
