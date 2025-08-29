# -*- coding: utf-8 -*-

"""
Copyright 2023 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import logging
import threading
import asyncio

from grpc import (
    UnaryUnaryClientInterceptor,
    UnaryStreamClientInterceptor,
    StreamUnaryClientInterceptor,
    StreamStreamClientInterceptor,
)

from anyio.from_thread import start_blocking_portal
from typing import Callable, Optional, Union, List, Any

from grpc import (
    StreamStreamClientInterceptor,
    StreamUnaryClientInterceptor,
    UnaryStreamClientInterceptor,
    UnaryUnaryClientInterceptor,
)

from dapr.common.pubsub.subscription import StreamInactiveError
from dapr.clients.retry import RetryPolicy
from dapr.version import __version__
from dapr.clients.grpc._response import InvokeMethodResponse, TopicEventResponse, DaprResponse
from dapr.aio.clients.grpc.client import DaprGrpcClientAsync
from dapr.clients.grpc._helpers import MetadataTuple
from dapr.clients.retry import RetryPolicy


class ResponseWrapper:
    """
    Wrapper for response objects that have async methods.
    Automatically converts async methods to sync ones.
    """

    def __init__(self, wrapped_obj, client):
        self._wrapped = wrapped_obj
        self._client = client

    def __getattr__(self, name):
        attr = getattr(self._wrapped, name)

        # List of known async method names that should be wrapped
        async_methods = {
            'read',  # CryptoResponse
            'close',  # Subscription
            'next_message',  # Subscription
            'respond',  # Subscription
            'respond_success',  # Subscription
            'respond_retry',  # Subscription
            'respond_drop',  # Subscription
        }

        # If it's a callable and potentially async, wrap it
        if callable(attr) and name in async_methods:
            def sync_wrapper(*args, **kwargs):
                async def _async_call():
                    return await attr(*args, **kwargs)
                return self._client._portal.call(_async_call)
            return sync_wrapper

        # For all other attributes, return as-is
        return attr


class DaprGrpcClient:
    """
    Synchronous wrapper around DaprGrpcClientAsync.

    This client automatically manages an async event loop in the background
    and provides a synchronous interface to all async Dapr operations.

    Usage:
        # As context manager (recommended)
        with DaprGrpcClient() as client:
            client.publish_event('pubsub', 'topic', b'data')

        # Manual lifecycle management
        client = DaprGrpcClient()
        client.publish_event('pubsub', 'topic', b'data')
        client.close()  # Remember to close!

    All methods from DaprGrpcClientAsync are automatically available
    in synchronous form via the __getattr__ magic method.
    """
    def __init__(
            self,
            address: Optional[str] = None,
            interceptors: Optional[
                List[
                    Union[
                        UnaryUnaryClientInterceptor,
                        UnaryStreamClientInterceptor,
                        StreamUnaryClientInterceptor,
                        StreamStreamClientInterceptor,
                    ]
                ]
            ] = None,
            max_grpc_message_length: Optional[int] = None,
            retry_policy: Optional[RetryPolicy] = None,
      ):
        self._portal_cm = None
        self._portal = None
        self._ac: Optional[DaprGrpcClientAsync] = None
        self._closed = True

        self._address = address
        self._interceptors = interceptors
        self._max_grpc_message_length = max_grpc_message_length
        self._retry_policy = retry_policy

    # ---- properties --------------------------------------------------------
    @property
    def is_open(self) -> bool:
        """Check if the client is open and ready for use."""
        return self._portal is not None and not self._closed

    @property
    def is_closed(self) -> bool:
        """Check if the client is closed."""
        return self._closed

    # ---- lifecycle ---------------------------------------------------------
    def __enter__(self) -> "DaprGrpcClient":
        self.open()
        return self

    def __exit__(self, et, ev, tb) -> None:
        self.close()

    def open(self) -> None:
        """Initialize the background event loop (but defer async client creation)."""
        self._guard_not_in_async_context()
        if self._portal_cm is not None:
            return

        try:
            # Start a dedicated background loop
            self._portal_cm = start_blocking_portal()
            self._portal = self._portal_cm.__enter__()
            self._closed = False
            # Don't create async client yet - defer until first use
        except Exception:
            # Cleanup on failure
            self._cleanup_portal()
            raise

    def _ensure_async_client(self) -> None:
        """Ensure the async client is created (lazy initialization)."""
        if self._ac is None:
            async def _build() -> DaprGrpcClientAsync:
                return DaprGrpcClientAsync(
                    self._address,
                    self._interceptors,
                    self._max_grpc_message_length,
                    self._retry_policy
                )

            self._ac = self._portal.call(_build)

    def close(self) -> None:
        """Close the async client and cleanup resources."""
        if self._portal_cm is None or self._closed:
            return

        try:
            if self._ac is not None:
                async def _shutdown() -> None:
                    assert self._ac is not None
                    await self._ac.close()

                self._portal.call(_shutdown)
        except Exception:
            # Log the error but continue cleanup
            logging.exception("Error during async client shutdown")
        finally:
            self._cleanup_portal()

    def _cleanup_portal(self) -> None:
        """Helper to cleanup portal resources."""
        if self._portal_cm is not None:
            try:
                self._portal_cm.__exit__(None, None, None)
            except Exception:
                pass  # Ignore cleanup errors

        self._portal_cm = None
        self._portal = None
        self._ac = None
        self._closed = True

    def __getattr__(self, name: str) -> Callable:
        """
        Automatically wrap any async method from the underlying client.
        This eliminates the need to manually define wrapper methods.
        """
        # Ensure we're initialized
        if self._portal is None:
            self.open()

        # Ensure async client is created
        self._ensure_async_client()

        # Check if the async client has this attribute
        if not hasattr(self._ac, name):
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

        attr = getattr(self._ac, name)

        # If it's a callable (method), check if it's async or sync
        if callable(attr):
                        # Check if this is a known sync method that shouldn't be wrapped
            sync_methods = {
                '_get_http_extension',  # HTTP utility method
                # Add other sync methods as needed
            }

            # Special cases that need custom handling
            special_methods = {
                'subscribe_with_handler',  # Complex async pattern, needs special wrapper
            }

            if name in sync_methods:
                # Return the method directly without async wrapping
                return attr
            elif name in special_methods:
                # Handle special cases with custom wrappers
                return self._create_special_wrapper(name, attr)

            # For async methods, return a sync wrapper
            def sync_wrapper(*args, **kwargs):
                return self._call(name, *args, **kwargs)

            # Copy some metadata for better debugging
            sync_wrapper.__name__ = name
            sync_wrapper.__doc__ = getattr(attr, '__doc__', None)
            return sync_wrapper

        # For non-callable attributes, return as-is
        return attr

    def _create_special_wrapper(self, method_name: str, async_method):
        """Create special wrappers for methods that need custom sync handling."""
        if method_name == 'subscribe_with_handler':
            def sync_subscribe_with_handler(pubsub_name, topic, handler_fn, metadata=None, dead_letter_topic=None):
                """
                Subscribe to a topic with a message handler function (sync version).

                Args:
                    pubsub_name (str): The name of the pubsub component.
                    topic (str): The name of the topic.
                    handler_fn (Callable): The function to call when a message is received.
                    metadata (Optional[dict]): Additional metadata for the subscription.
                    dead_letter_topic (Optional[str]): Name of the dead-letter topic.

                Returns:
                    Callable: A sync function to close the subscription.
                """

                # Ensure we're open and have an async client
                if self._portal is None:
                    self.open()
                self._ensure_async_client()

                # Create the subscription in the async portal
                async def _setup_subscription():
                    subscription = await self._ac.subscribe(pubsub_name, topic, metadata, dead_letter_topic)

                    async def stream_messages():
                        try:
                            async for message in subscription:
                                if message:
                                    # Call the handler function (assume it's sync)
                                    response = handler_fn(message)
                                    if response:
                                        await subscription.respond(message, response.status)
                                else:
                                    continue
                        except StreamInactiveError:
                            pass  # Subscription was closed

                    # Start the message handling task
                    task = asyncio.create_task(stream_messages())

                    return subscription, task

                # Set up the subscription and get task reference
                subscription, task = self._portal.call(_setup_subscription)

                def close_subscription():
                    """Close the subscription (sync version)."""
                    async def _close():
                        # Cancel the task first
                        if not task.done():
                            task.cancel()
                            try:
                                await task
                            except asyncio.CancelledError:
                                pass
                        # Then close the subscription
                        await subscription.close()

                    self._portal.call(_close)

                return close_subscription

            return sync_subscribe_with_handler

        # For other special methods, add handling here
        return None

    # ---- tiny helper to forward calls into the async loop ------------------
    def _call(self, fn: Union[str, Callable[..., Any]], *args, **kwargs):
        """
        Call an async method (bound to self._ac) inside the background loop
        and block until it completes, returning the result.
        """
        if self._portal is None:
            # Allow implicit open() if user forgot the context manager
            self.open()

        # Ensure async client is created
        self._ensure_async_client()

        # If fn is a string, get the method from self._ac
        if isinstance(fn, str):
            method_name = fn
            if not hasattr(self._ac, method_name):
                raise AttributeError(f"Async client has no method '{method_name}'")
            fn = getattr(self._ac, method_name)

        if not callable(fn):
            raise TypeError(f"'{fn}' is not callable")

        try:
            # Create an async wrapper that properly handles the method call
            async def _async_wrapper():
                result = await fn(*args, **kwargs)
                # If the result has async methods, wrap them too
                return self._wrap_response_if_needed(result)

            return self._portal.call(_async_wrapper)
        except Exception as e:
            # Re-raise with better context if needed
            raise

    def _wrap_response_if_needed(self, obj):
        """
        Wrap response objects that have async methods to make them sync.
        """
        # Special case: TryLockResponse has a reference to the async client
        # We need to replace it with a reference to our sync client
        if hasattr(obj, '_client') and hasattr(obj, '_store_name') and hasattr(obj, '_resource_id'):
            # This looks like a TryLockResponse - replace the client reference
            obj._client = self
            return obj

        # Check if the object has async methods that need wrapping
        obj_module = getattr(type(obj), '__module__', '')

        # Check for objects from the async client that have async methods
        if 'aio' in obj_module:
            # Check for known async methods
            async_methods = {'read', 'close', 'next_message', 'respond', 'respond_success', 'respond_retry', 'respond_drop'}
            has_async_methods = any(hasattr(obj, method) and callable(getattr(obj, method)) for method in async_methods)

            if has_async_methods:
                return ResponseWrapper(obj, self)

        return obj

    @staticmethod
    def _guard_not_in_async_context():
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        # You're already in an event loop: using the sync client would deadlock/footgun.
        raise RuntimeError("You're in async code—use dapr.aio.AsyncClient here.")
