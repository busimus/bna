from decimal import Decimal
from bna.transfer import Transfer

bridge_b = {
		'type': 'transfer',
		'by': '0xdc068f57bf8a16583d0e228deb242f47e3b999e0',
		'amount': Decimal(
			'40013.925619'
		),
		'tfs': [
			Transfer(
				changes={
					'0xdc068f57bf8a16583d0e228deb242f47e3b999e0': Decimal(
						'-40013.925619'
					),
					'0x0000000000000000000000000000000000000000': Decimal(
						'40013.925619'
					)
				},
				hash='0xc7308787c7d16fb068ed7b623eb6233035fd1642a388d86b31ad3a6fc935ae6f',
				blockNumber=20170708,
				logIndex=374,
				timeStamp=1659689308,
				chain='bsc',
				tags=[
					'bridge_burn'
				],
				meta={
				}
			)
		],
		'time': 1659685621
	}
bridge_i = {
		'type': 'transfer',
		'by': '0x98d16d7021930b788135dd834983394ff2de9869',
		'amount': Decimal(
			'36907.925618999999332352'
		),
		'tfs': [
			Transfer(
				changes={
					'0x98d16d7021930b788135dd834983394ff2de9869': Decimal(
						'-40012.925618999999332352'
					),
					'0x0bf957b7c580216667d009006d42378caf633f2b': Decimal(
						'40012.925618999999332352'
					)
				},
				hash='0x29a2e910d6bebdc1cdba2e45e6387894efc5b00f0f2d37e25a991834e702be05',
				blockNumber=4767450,
				logIndex=0,
				timeStamp=1659689332,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			),
			Transfer(
				changes={
					'0xb3c3cd8484c7d5d0533f97ea7cc0d717760e4e02': Decimal(
						'-3105'
					),
					'0x98d16d7021930b788135dd834983394ff2de9869': Decimal(
						'3105'
					)
				},
				hash='0xe54a3b0ade207760153a037427cbcf4726892e22d1d6a22e1ec1841a4ad8de5d',
				blockNumber=4767485,
				logIndex=0,
				timeStamp=1659690032,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			)
		],
		'time': 1659685621
	}
simple = {
		'type': 'transfer',
		'by': '0xb3c3cd8484c7d5d0533f97ea7cc0d717760e4e02',
		'amount': Decimal(
			'3105'
		),
		'tfs': [
			Transfer(
				changes={
					'0xb3c3cd8484c7d5d0533f97ea7cc0d717760e4e02': Decimal(
						'-3105'
					),
					'0x98d16d7021930b788135dd834983394ff2de9869': Decimal(
						'3105'
					)
				},
				hash='0xe54a3b0ade207760153a037427cbcf4726892e22d1d6a22e1ec1841a4ad8de5d',
				blockNumber=4767485,
				logIndex=0,
				timeStamp=1659690032,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			)
		],
		'time': 1659685621
	}
multi1_recv = {
		'type': 'transfer',
		'by': '0x8ab52e639aea66264bf8008eaf8b34232ca449c4',
		'amount': Decimal(
			'391'
		),
		'tfs': [
			Transfer(
				changes={
					'0x8ab52e639aea66264bf8008eaf8b34232ca449c4': Decimal(
						'-391'
					),
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'391'
					)
				},
				hash='0xcee8e4f382e173fada2c50a0355c5f897332c81f37362191b93734876e73e891',
				blockNumber=4767567,
				logIndex=1,
				timeStamp=1659691675,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			)
		],
		'time': 1659685621
	}
multi1_send = {
		'type': 'transfer',
		'by': '0xf574f90696590563105c99e67559e4eaf38b8d5f',
		'amount': Decimal(
			'379'
		),
		'tfs': [
			Transfer(
				changes={
					'0x8ab52e639aea66264bf8008eaf8b34232ca449c4': Decimal(
						'-391'
					),
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'391'
					)
				},
				hash='0xcee8e4f382e173fada2c50a0355c5f897332c81f37362191b93734876e73e891',
				blockNumber=4767567,
				logIndex=1,
				timeStamp=1659691675,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			),
			Transfer(
				changes={
					'0x468ee19d8633e2e8f005caf98810f202a2da9054': Decimal(
						'-12'
					),
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'12'
					)
				},
				hash='0x652fbcb0ea74a24c0445940c4eb76024b26ec3f852388df4a1e57f57c48aeae2',
				blockNumber=4767573,
				logIndex=0,
				timeStamp=1659691792,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			),
			Transfer(
				changes={
					'0x71aaaef55b56f106aae7f916d31e024830cdee1b': Decimal(
						'-11'
					),
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'11'
					)
				},
				hash='0x2f2c2eaf1290479b0beabde7c779af37f107a9c0d8ce2fc8c6cfcb6f52a03fb2',
				blockNumber=4767574,
				logIndex=0,
				timeStamp=1659691812,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			),
			Transfer(
				changes={
					'0x7506c7441468c8a99ad6361a2676695d26badbbe': Decimal(
						'-10'
					),
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'10'
					)
				},
				hash='0x992ee7364a13f6c985d80136335cb9c1da1fa7d4d7427160819f1f8e38600c06',
				blockNumber=4767576,
				logIndex=0,
				timeStamp=1659691852,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			),
			Transfer(
				changes={
					'0x1bc975f65f2d70aecac362da0e24e578cb4d58b3': Decimal(
						'-12'
					),
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'12'
					)
				},
				hash='0xf16eadfffd51f3552ae7e33bb7604e743817acfb355431cc61080c771b464825',
				blockNumber=4767577,
				logIndex=0,
				timeStamp=1659691872,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			),
			Transfer(
				changes={
					'0x902a22e29ecb787c1cca89d78de0c1af6e525c57': Decimal(
						'-12'
					),
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'12'
					)
				},
				hash='0x0406cfb777e7a698663de2a52bc729e4a9041cfd749edf9399c46991f0df8232',
				blockNumber=4767579,
				logIndex=0,
				timeStamp=1659691912,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			),
			Transfer(
				changes={
					'0xf574f90696590563105c99e67559e4eaf38b8d5f': Decimal(
						'-827'
					),
					'0xfc698f346cfed9728521dfa6a7f47e6a6446b4d5': Decimal(
						'827'
					)
				},
				hash='0x2b9ffb43da952fc3eaf1298dad87e60b27b5c6d84cca5d7204e102d877e391d9',
				blockNumber=4767592,
				logIndex=0,
				timeStamp=1659692170,
				chain='idena',
				tags=[
					'send'
				],
				meta={
				}
			)
		],
		'time': 1659685621
	}

withdraw_lp = {
	'type': 'transfer',
	'by': '0xc1bcdc9eb37d8e72ff0e0ca4bc8d19735b1b38ce',
	'amount': Decimal(
		'1109.532898033552727407'
	),
	'tfs': [
		Transfer(
			changes={
				'0xc1bcdc9eb37d8e72ff0e0ca4bc8d19735b1b38ce': Decimal(
					'-1109.532898033552727407'
				),
				'0x09784d03b42581cfc4fc90a7ab11c3125dedeb86': Decimal(
					'1109.532898033552727407'
				)
			},
			hash='0x781beb7989be955d97504a1a5ee985429fd5d20efdbbee71ab9c52747e9a271f',
			blockNumber=20171806,
			logIndex=148,
			timeStamp=1659692615,
			chain='bsc',
			tags=[
				'dex_withdraw_lp',
				'dex_buy',
				'dex'
			],
			meta={
			}
		)
	],
	'time': 1659686162
}
