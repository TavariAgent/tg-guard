from ...token_system import task_token_guard


@task_token_guard(
    operation_type='trivial_math',
    tags={'weight': 'light'}
)
def trivial_operation(x):
    """Instant: Simple arithmetic."""
    return x ** 2


@task_token_guard(
    operation_type='simple_loop',
    tags={'weight': 'light'}
)
def simple_operation(n):
    """Fast: Basic loop with accumulation."""
    total = 0
    for i in range(n):
        total += i
    return total


@task_token_guard(
    operation_type='list_process',
    tags={'weight': 'medium'}
)
def moderate_operation(size):
    """Medium: List comprehension and filtering."""
    data = [i * 2 for i in range(size)]
    filtered = [x for x in data if x % 3 == 0]
    return sum(filtered)


@task_token_guard(
    operation_type='nested_loops',
    tags={'weight': 'medium',
          "process_pool": True}
)
def complex_operation(dimension):
    """Slower: Nested loops with matrix-like structure."""
    matrix = []
    for i in range(dimension):
        row = []
        for j in range(dimension):
            row.append(i * j)
        matrix.append(row)
    return sum(sum(row) for row in matrix)


@task_token_guard(
    operation_type='cpu_intensive',
    tags={'weight': 'heavy',
          "process_pool": True}
)
def heavy_operation(iterations):
    """Slow: CPU-intensive calculation."""
    result = 0
    for i in range(iterations):
        result += sum(j ** 2 for j in range(100))
    return result


@task_token_guard(
    operation_type='fibonacci',
    tags={'weight': 'heavy',
          "process_pool": True}
)
def fibonacci_operation(n):
    """Iterative fibonacci (intentionally inefficient for testing)."""
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


@task_token_guard(
    operation_type='prime_check',
    tags={'weight': 'medium'}
)
def prime_operation(n):
    """Check if the number is prime (moderate complexity)."""
    if n < 2:
        return False
    for i in range(2, int(n ** 0.5) + 1):
        if n % i == 0:
            return False
    return True


@task_token_guard(
    operation_type='string_ops',
    tags={'weight': 'light'}
)
def string_operation(length):
    """String manipulation operations."""
    s = "test" * length
    return len(s.upper().replace("T", "X").split("X"))