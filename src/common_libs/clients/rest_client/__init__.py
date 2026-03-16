import nest_asyncio2

from .ext import JSONType, RestResponse
from .rest_client import AsyncRestClient, RestClient

nest_asyncio2.apply(run_close_loop=True)
