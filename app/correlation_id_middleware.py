# correlation_id_middleware.py
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response
import uuid
import logging

logger = logging.getLogger(__name__)

class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        try:
            correlation_id = request.headers.get('X-Correlation-ID')
            if not correlation_id:
                correlation_id = str(uuid.uuid4())
                logger.info(f"Generated new Correlation ID: {correlation_id}")
            else:
                logger.info(f"Received Correlation ID from header: {correlation_id}")

            request.state.correlation_id = correlation_id
            response: Response = await call_next(request)
            response.headers['X-Correlation-ID'] = correlation_id
            logger.info(f"Set Correlation ID in response headers: {correlation_id}")
            return response
        except Exception as e:
            logger.error(f"Error in CorrelationIdMiddleware: {e}")
            raise