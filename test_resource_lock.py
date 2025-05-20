from AzHelper import AzureResourceLocker

def test_resource_locker():
    """
    Test AzureResourceLocker functionality:
    1. Get initial locks
    2. Create a new lock
    3. Delete all locks
    4. Recreate all locks
    """
    print("\nTesting AzureResourceLocker...")
    
    # Initialize the locker
    locker = AzureResourceLocker(resource_group_name="adf")
    print("\nInitial lock_objs:", [lock.name for lock in locker.lock_objs])
    
    # Test 1: Get initial locks
    print("\nTest 1: Getting initial locks")
    initial_locks = locker.lock_objs
    print(f"Initial locks count: {len(initial_locks)}")
    print("Initial lock_objs:", [lock.name for lock in locker.lock_objs])
    
    # Test 2: Create a new lock
    print("\nTest 2: Creating a new lock")
    test_lock_name = "test-lock"
    locker.create_resource_lock(
        lock_name=test_lock_name,
        level="CanNotDelete",
        notes="Test lock for automation"
    )
    print("After create lock_objs:", [lock.name for lock in locker.lock_objs])
    
    # Verify the lock was created
    current_locks = locker.get_locks()  # Get current locks without updating lock_objs
    print("Current locks from get_locks:", [lock.name for lock in current_locks])
    found_lock = False
    for lock in current_locks:
        if lock.name == test_lock_name:
            found_lock = True
            print(f"Found created lock: {lock.name} with level {lock.level}")
            break
    assert found_lock, "Created lock not found in lock list"
    
    # Test 3: Delete all locks
    print("\nTest 3: Deleting all locks")
    locker.delete_resource_locks()
    print("After delete lock_objs:", [lock.name for lock in locker.lock_objs])
    assert locker.deleted, "Locks were not marked as deleted"
    
    # Verify locks were deleted
    current_locks = locker.get_locks()  # Get current locks without updating lock_objs
    print("Current locks after delete:", [lock.name for lock in current_locks])
    assert len(current_locks) == 0, "Locks were not deleted"
    
    # Test 4: Recreate all locks
    print("\nTest 4: Recreating all locks")
    locker.recreate_locks()
    print("After recreate lock_objs:", [lock.name for lock in locker.lock_objs])
    
    # Verify locks were recreated
    current_locks = locker.get_locks()  # Get current locks without updating lock_objs
    print("Current locks after recreate:", [lock.name for lock in current_locks])
    assert len(current_locks) == len(initial_locks) + 1, "Locks were not recreated correctly"
    
    # Verify the test lock was recreated
    found_lock = False
    for lock in current_locks:
        if lock.name == test_lock_name:
            found_lock = True
            print(f"Found recreated lock: {lock.name} with level {lock.level}")
            break
    assert found_lock, "Test lock was not recreated"
    
    print("\nAll AzureResourceLocker tests completed successfully!")

if __name__ == "__main__":
    test_resource_locker() 