# ADR 001: Hexagonal modules

**Status:** Accepted

Keep domain rules and application ports in framework-free Maven modules. Spring applications and outbound adapters depend inward. This makes transaction and delivery policies testable without hiding them behind framework annotations.
