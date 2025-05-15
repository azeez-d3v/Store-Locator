import asyncio
from typing import Dict, List, Any, Optional, Union
from curl_cffi import AsyncSession

class SessionManager:
    """
    Session manager for making asynchronous HTTP requests using curl_cffi.
    """
    
    def __init__(self, default_headers: Optional[Dict[str, str]] = None):
        """
        Initialize the session manager.
        
        Args:
            default_headers: Default headers to use for all requests.
        """
        self.default_headers = default_headers or {}
        
    async def make_requests(self, 
                           requests: List[Dict[str, Any]]) -> List[Any]:
        """
        Make multiple requests concurrently.
        
        Args:
            requests: List of request configurations, each containing:
                - url: The URL to request
                - method: HTTP method (default: 'GET')
                - headers: Optional headers (combined with default_headers)
                - data: Optional request payload
                - json: Optional JSON payload (will be serialized)
                
        Returns:
            List of response objects
        """
        async with AsyncSession(impersonate="edge101") as session:
            tasks = []
            for req in requests:
                url = req['url']
                method = req.get('method', 'GET').lower()
                
                # Combine default headers with request-specific headers
                headers = {**self.default_headers}
                if 'headers' in req:
                    headers.update(req['headers'])
                
                # Build request kwargs
                kwargs = {'headers': headers}
                
                if 'data' in req:
                    kwargs['data'] = req['data']
                    
                if 'json' in req:
                    kwargs['json'] = req['json']
                
                # Create appropriate request task based on method
                http_method = getattr(session, method)
                task = http_method(url, **kwargs)
                tasks.append(task)
                
            return await asyncio.gather(*tasks, return_exceptions=True)
    
    async def get(self, url: str, headers: Optional[Dict[str, str]] = None) -> Any:
        """
        Make a GET request.
        
        Args:
            url: The URL to request
            headers: Optional headers
            
        Returns:
            Response object
        """
        async with AsyncSession(impersonate="edge101") as session:
            combined_headers = {**self.default_headers}
            
            if headers:
                combined_headers.update(headers)
            return await session.get(url, headers=combined_headers)
    
    async def post(self, url: str, 
                  data: Optional[Union[str, Dict]] = None,
                  json: Optional[Dict] = None,
                  headers: Optional[Dict[str, str]] = None) -> Any:
        """
        Make a POST request.
        
        Args:
            url: The URL to request
            data: Optional request payload
            json: Optional JSON payload
            headers: Optional headers
            
        Returns:
            Response object
        """
        async with AsyncSession(impersonate="edge101") as session:
            combined_headers = {**self.default_headers}
            if headers:
                combined_headers.update(headers)
            
            kwargs = {'headers': combined_headers}
            if data is not None:
                kwargs['data'] = data
            if json is not None:
                kwargs['json'] = json
                
            return await session.post(url, **kwargs)