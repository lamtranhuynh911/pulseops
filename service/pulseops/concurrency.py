"""asyncio helpers."""

import asyncio


async def gather_all(*aws):
    """Like asyncio.gather, but lets every awaitable finish before re-raising the first error.

    With plain gather, the other writes keep running after one fails. The worker then retries the
    message while a write from the first attempt can still land, out of order with later messages.
    """
    results = await asyncio.gather(*aws, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result
    return results
