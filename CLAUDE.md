# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This project uses Hatch as the build system:

- **Type checking**: `hatch run types:check` - Runs mypy type checking on source and tests
- **Testing**: `python -m pytest tests/` - Run all tests (uses pytest)
- **Single test**: `python -m pytest tests/test_<module>.py` - Run specific test file
- **Coverage**: `python -m coverage run -m pytest tests/` then `python -m coverage report` - Generate coverage reports

## Architecture Overview

HOT Redis is a Python wrapper library for Redis that provides higher-level data types mimicking Python built-ins (list, dict, set, etc.) backed by Redis operations. The library emphasizes atomic operations through Lua scripting.

### Core Components

**Client System** (`client.py`):
- `HotClient`: Redis client wrapper that auto-loads Lua functions from `lua/` directory
- `default_client()`: Thread-local client factory with global configuration
- `configure()`: Global Redis connection configuration
- `transaction()`: Context manager for Redis pipeline transactions

**Type Hierarchy** (`types.py`):
- Base classes: `Base`, `Sequential`, `Numeric`, `Bitwise` - provide operator overloading
- Core types: `List`, `Set`, `Dict`, `String`, `Int`, `Float` - mirror Python built-ins
- Concurrency types: `Queue`, `Lock`, `Semaphore`, `BoundedSemaphore`, `RLock` - thread-safe primitives
- Collection types: `DefaultDict`, `MultiSet` - advanced data structures

**Specialized Data Structures**:
- `DelayButFastSet` (`fast_set.py`): Generic cached Redis set with periodic refresh and version tracking
- `DebounceTask`/`DebounceInfoTask` (`debounce_task.py`): Debounced task queuing system using sorted sets
- `RedisRange` (`redis_range.py`): Range-based operations

### Key Design Patterns

1. **Atomic Operations**: Many methods use Lua scripts (in `lua/atoms.lua`) to ensure atomicity
2. **Thread Safety**: Thread-local client storage with optional global configuration
3. **Python Compatibility**: All types implement standard Python interfaces and magic methods
4. **Generic Types**: New components use TypeVar for type safety (e.g., `DelayButFastSet[T]`)

### Testing Structure

Tests are organized by module:
- `test_debounce_task.py` - Debounce functionality
- `test_fast_set.py` - DelayButFastSet operations  
- `test_redis_range.py` - Range operations
- `test_types.py` - Core type system
- `tests.py` - Additional integration tests

When working with this codebase:
- All Redis operations should maintain atomicity where possible
- New types should follow the Base class pattern and implement appropriate Python magic methods
- Use Lua scripts for complex multi-step operations
- Consider thread safety and use transaction contexts for multi-operation sequences