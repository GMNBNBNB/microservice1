# log_requests_middleware.py
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response
import time
import logging

logger = logging.getLogger(__name__)

class LogRequestsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # get Correlation ID
        correlation_id = getattr(request.state, 'correlation_id', 'N/A')

        logger.info(f"Request: {request.method} {request.url} | Correlation ID: {correlation_id}")

        # record time
        start_time = time.time()

        response: Response = await call_next(request)

        # calculate time
        process_time = time.time() - start_time

        logger.info(f"Response status: {response.status_code} | Time: {process_time:.4f}s | Correlation ID: {correlation_id}")

        return response
