---
'@mastra/auth-better-auth': patch
---

Fixed deferred authentication with custom database adapters by leaving schema initialization to the adapter. Built-in PostgreSQL and LibSQL migrations keep their existing behavior.
