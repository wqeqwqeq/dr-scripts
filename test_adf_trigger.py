from AzHelper import ADFTrigger
from datetime import datetime, timedelta

def test_adf_trigger():
    """
    Test ADFTrigger functionality:
    1. List all triggers
    2. List tumbling triggers
    3. List schedule triggers
    4. Stop all triggers
    5. Start all triggers
    6. Reset a tumbling trigger's start time
    """
    print("\nTesting ADFTrigger...")
    
    # Initialize the trigger manager
    trigger_mgr = ADFTrigger(resource_group_name="adf", resource_name="adf-stanley")
    print("\nInitial triggers:", [t.name for t in trigger_mgr.list_triggers()])
    
    # Test 1: List all triggers
    print("\nTest 1: Listing all triggers")
    all_triggers = trigger_mgr.list_triggers()
    print(f"All triggers count: {len(all_triggers)}")
    print("All triggers:", [t.name for t in all_triggers])
    assert len(all_triggers) > 0, "No triggers found"
    
    # Test 2: List tumbling triggers
    print("\nTest 2: Listing tumbling triggers")
    tumbling_triggers = trigger_mgr.list_triggers(trigger_type="TumblingWindowTrigger")
    print(f"Tumbling triggers count: {len(tumbling_triggers)}")
    print("Tumbling triggers:", [t.name for t in tumbling_triggers])
    
    # Test 3: List schedule triggers
    print("\nTest 3: Listing schedule triggers")
    schedule_triggers = trigger_mgr.list_triggers(trigger_type="ScheduleTrigger")
    print(f"Schedule triggers count: {len(schedule_triggers)}")
    print("Schedule triggers:", [t.name for t in schedule_triggers])
    
    # Test 4: Stop all triggers
    print("\nTest 4: Stopping all triggers")
    trigger_mgr.manage_all_triggers("stop")
    print("All triggers stopped")
    
    # Verify triggers are stopped
    current_triggers = trigger_mgr.list_triggers()
    for trigger in current_triggers:
        trigger_obj = trigger_mgr.client.triggers.get(
            trigger_mgr.resource_group_name,
            trigger_mgr.resource_name,
            trigger.name
        )
        assert trigger_obj.properties.runtime_state == "Stopped", f"Trigger {trigger.name} is not stopped"
    
    # Test 5: Start all triggers
    print("\nTest 5: Starting all triggers")
    trigger_mgr.manage_all_triggers("start")
    print("All triggers started")
    
    # Verify triggers are started
    current_triggers = trigger_mgr.list_triggers()
    for trigger in current_triggers:
        trigger_obj = trigger_mgr.client.triggers.get(
            trigger_mgr.resource_group_name,
            trigger_mgr.resource_name,
            trigger.name
        )
        assert trigger_obj.properties.runtime_state == "Started", f"Trigger {trigger.name} is not started"
    
    # Test 6: Reset tumbling trigger start time
    if tumbling_triggers:
        print("\nTest 6: Resetting tumbling trigger start time")
        test_trigger = tumbling_triggers[0]
        new_start_time = datetime.now() + timedelta(days=1)
        print(f"Resetting trigger {test_trigger.name} to start at {new_start_time}")
        
        # Store original start time
        original_trigger = trigger_mgr.client.triggers.get(
            trigger_mgr.resource_group_name,
            trigger_mgr.resource_name,
            test_trigger.name
        )
        original_start_time = original_trigger.properties.start_time
        
        # Reset start time
        trigger_mgr.reset_tumbling_with_start_time(test_trigger.name, new_start_time)
        
        # Verify start time was updated
        updated_trigger = trigger_mgr.client.triggers.get(
            trigger_mgr.resource_group_name,
            trigger_mgr.resource_name,
            test_trigger.name
        )
        assert updated_trigger.properties.start_time != original_start_time, "Start time was not updated"
        print(f"Successfully reset start time for trigger {test_trigger.name}")
    else:
        print("\nTest 6: Skipped - No tumbling triggers found")
    
    print("\nAll ADFTrigger tests completed successfully!")

if __name__ == "__main__":
    test_adf_trigger() 