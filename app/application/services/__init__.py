"""Application services.

Services are imported from their concrete modules. Keeping this package free of
eager imports prevents optional legacy integrations from loading when a focused
workspace service is used.
"""
