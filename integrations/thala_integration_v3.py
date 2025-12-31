import logging
from typing import Dict, List, Optional
import requests

from dotenv import load_dotenv
from constants.summary_columns import SummaryColumn
from constants.example_integrations import (
    THALA_SUSDE_V3_START_BLOCK,
)
from constants.thala import (
    ETHENA_V3_BALANCES_API_URL,
    SUSDE_POOL_ADDRESS,
)
from constants.chains import Chain
from integrations.integration_ids import IntegrationID as IntID
from integrations.l2_delegation_integration import L2DelegationIntegration

load_dotenv()


class ThalaV3AptosIntegration(L2DelegationIntegration):
    def __init__(
        self,
        integration_id: IntID,
        start_block: int,
        chain: Chain = Chain.APTOS,
        reward_multiplier: int = 1,
    ):
        super().__init__(
            integration_id=integration_id,
            start_block=start_block,
            chain=chain,
            summary_cols=[SummaryColumn.THALA_SHARDS],
            reward_multiplier=reward_multiplier,
        )

    def get_l2_block_balances(
        self, cached_data: Dict[int, Dict[str, float]], blocks: List[int]
    ) -> Dict[int, Dict[str, float]]:
        logging.info("Getting block data for Thala sUSDe LP...")
        # Ensure blocks are sorted smallest to largest
        block_data: Dict[int, Dict[str, float]] = {}
        sorted_blocks = sorted(blocks)

        # Populate block data from smallest to largest
        for block in sorted_blocks:
            result = self.get_thala_block_data(block)

            # Store the balances and cache the exchange rate
            if result:
                block_data[block] = result

        return block_data

    def get_thala_block_data(
        self, block: int
    ):
        print("Getting participants data for block: ", block)
        try:
            response = requests.get(
                ETHENA_V3_BALANCES_API_URL,
                params={
                    "poolAddress": SUSDE_POOL_ADDRESS,
                    "blockNumber": block,
                },
                timeout=10,
            )
            response.raise_for_status()

            result = response.json()
            return result.get("data", {})

        except requests.RequestException as e:
            logging.error(f"Request failed for block {block}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error for block {block}: {str(e)}")
            raise


if __name__ == "__main__":
    example_integration = ThalaV3AptosIntegration(
        integration_id=IntID.THALA_SUSDE_V3_LP,
        start_block=THALA_SUSDE_V3_START_BLOCK,
        chain=Chain.APTOS,
        reward_multiplier=5,
    )

    example_integration_output = example_integration.get_l2_block_balances(
        cached_data={},
        blocks=list(range(THALA_SUSDE_V3_START_BLOCK, THALA_SUSDE_V3_START_BLOCK + 15306000, 1500000)),
    )

    print("=" * 120)
    print("Run without cached data", example_integration_output)
    print("=" * 120, "\n" * 5)
