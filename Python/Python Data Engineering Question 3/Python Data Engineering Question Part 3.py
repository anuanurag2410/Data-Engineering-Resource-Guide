# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Question 1
# MAGIC
# MAGIC ### Nested Dictionary

# COMMAND ----------

Dict={

    "Name":"Anurag",
    "Subjects":["Maths","Science","English"]
    "Details":{
        "Age":25,
        "Height":170
        "Email":"anurag@gmail.com"
        "Number":98689669956
        "Address":{
            "City":"Delhi",
            "Pincode":110001
            "State":"Delhi"
            "Country":"India"
                }
}
}

# COMMAND ----------


# QUESTION 1 — SOURCE DATA GENERATION

orders = [
    {
        "order_id": "O101",
        "customer": {
            "id": 1,
            "name": "Anurag"
        },
        "items": [
            {
                "sku": "P01",
                "quantity": 2,
                "unit_price": 100
            },
            {
                "sku": "P02",
                "quantity": 1,
                "unit_price": 300
            }
        ]
    },
    {
        "order_id": "O102",
        "customer": {
            "id": 2,
            "name": "Stuti"
        },
        "items": [
            {
                "sku": "P03",
                "quantity": 3,
                "unit_price": 150
            }
        ]
    },
    {
        "order_id": "O103",
        "customer": {
            "id": 3,
            "name": "Shalini"
        },
        "items": []
    }
]

print("SOURCE DATA — NESTED API RESPONSE")



# COMMAND ----------

display(orders)

# COMMAND ----------

flattened_orders=[]

for order in orders:
    order_id=order["order_id"]
    customer_name=order['customer']['name']
    
    for item in order['items']:
        sku=item['sku']
        quantity=item['quantity']
        unit_price=item['unit_price']
        line_amount=quantity*unit_price
        
        flattened_orders.append({
            "order_id":order_id,
            "customer_name":customer_name,
            "sku":sku,
            "quantity":quantity,
            "unit_price":unit_price,
            "line_amount":line_amount
        })

# COMMAND ----------

df=spark.createDataFrame(flattened_orders)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Question 2: Remove duplicate events and keep the latest record

# COMMAND ----------


# QUESTION 2 — SOURCE DATA GENERATION

events = [
    {
        "event_id": 1,
        "order_id": "O101",
        "status": "Pending",
        "updated_at": "2026-11-01T09:00:00",
        "version": 1
    },
    {
        "event_id": 2,
        "order_id": "O101",
        "status": "Completed",
        "updated_at": "2026-11-01T10:00:00",
        "version": 2
    },
    {
        "event_id": 2,
        "order_id": "O101",
        "status": "Completed",
        "updated_at": "2026-11-01T10:00:00",
        "version": 2
    },  # Duplicate event
    {
        "event_id": 3,
        "order_id": "O102",
        "status": "Pending",
        "updated_at": "2026-11-01T11:00:00",
        "version": 1
    },
    {
        "event_id": 4,
        "order_id": "O102",
        "status": "Completed",
        "updated_at": "2026-11-01T12:00:00",
        "version": 2
    },
    {
        "event_id": 5,
        "order_id": "O102",
        "status": "Cancelled",
        "updated_at": "2026-11-01T12:00:00",
        "version": 3
    },
    {
        "event_id": 6,
        "order_id": "O103",
        "status": "Completed",
        "updated_at": "2026-11-02T08:00:00",
        "version": 1
    },
    {
        "event_id": 6,
        "order_id": "O103",
        "status": "Completed",
        "updated_at": "2026-11-02T08:00:00",
        "version": 1
    }  # Duplicate event
]

print("SOURCE DATA — ORDER EVENTS")
for event in events:
    print(event)

display(events)


# COMMAND ----------

unique_events={}

for event in events:
    event_id=event["event_id"]


    if event_id in unique_events:
        if unique_events[event_id]!=event:
            raise ValueError(f"Conflicting Data for Event_id {event_id}")
        continue

    unique_events[event_id]=event

display(unique_events)

# COMMAND ----------

latest_orders={}


for event in unique_events.values():
    order_id = event["order_id"]

    current_key = (
        event["updated_at"],
        event["version"],
        event["event_id"]
    )

    if order_id not in latest_orders:
        latest_orders[order_id] = event
        continue

    previous_event = latest_orders[order_id]

    previous_key = (
        previous_event["updated_at"],
        previous_event["version"],
        previous_event["event_id"]
    )

    if current_key > previous_key:
        latest_orders[order_id] = event

# Step 3: Display final order states
print("\nLATEST ORDER STATES")
for order_id in sorted(latest_orders):
    event = latest_orders[order_id]
    print(
        order_id,
        event["status"],
        event["version"]
    )

# Step 4: Validate
assert len(events) == 8
assert len(unique_events) == 6
assert len(latest_orders) == 3

assert latest_orders["O101"]["status"] == "Completed"
assert latest_orders["O102"]["status"] == "Cancelled"
assert latest_orders["O103"]["status"] == "Completed"


# COMMAND ----------

# MAGIC %md
# MAGIC # Question 3: Source-to-target data reconciliation

# COMMAND ----------


# QUESTION 3 — SOURCE DATA GENERATION

source_orders = [
    {"order_id": "O101", "amount": 1200},
    {"order_id": "O102", "amount": 950},
    {"order_id": "O103", "amount": 600},
    {"order_id": "O104", "amount": 400}
]

target_orders = [
    {"order_id": "O101", "amount": 1200},
    {"order_id": "O102", "amount": 900},
    {"order_id": "O105", "amount": 700}
]

print("SOURCE DATA")
for record in source_orders:
    print(record)

print("\nTARGET DATA")
for record in target_orders:
    print(record)


# COMMAND ----------

source_dict={

    record["order_id"]:record["amount"] for record in source_orders
}
display(source_dict)

# COMMAND ----------

target_dict = {
    record["order_id"]: record["amount"]
    for record in target_orders
}

display(target_dict)

# COMMAND ----------

assert len(source_dict) == len(source_orders)
assert len(target_dict) == len(target_orders)


# COMMAND ----------

source_keys=set(source_dict)
target_keys=set(target_dict)

extra_in_target = sorted(
    target_keys - source_keys
)


# COMMAND ----------

print(extra_in_target)

# COMMAND ----------


# QUESTION 3 — PYTHON SOLUTION

# Step 1: Convert lists into lookup dictionaries
source_dict = {
    record["order_id"]: record["amount"]
    for record in source_orders
}

target_dict = {
    record["order_id"]: record["amount"]
    for record in target_orders
}

# Ensure the unique-key assumption is satisfied
assert len(source_dict) == len(source_orders)
assert len(target_dict) == len(target_orders)

# Step 2: Compare source and target keys
source_keys = set(source_dict)
target_keys = set(target_dict)

missing_in_target = sorted(
    source_keys - target_keys
)

extra_in_target = sorted(
    target_keys - source_keys
)

# Step 3: Compare amounts for common records
amount_mismatches = []

for order_id in sorted(source_keys & target_keys):
    source_amount = source_dict[order_id]
    target_amount = target_dict[order_id]

    if source_amount != target_amount:
        amount_mismatches.append({
            "order_id": order_id,
            "source_amount": source_amount,
            "target_amount": target_amount
        })

# Step 4: Display reconciliation report
print("MISSING IN TARGET")
for order_id in missing_in_target:
    print(order_id, source_dict[order_id])

print("\nEXTRA IN TARGET")
for order_id in extra_in_target:
    print(order_id, target_dict[order_id])

print("\nAMOUNT MISMATCHES")
for record in amount_mismatches:
    print(record)

# Step 5: Validate expected results
assert missing_in_target == ["O103", "O104"]
assert extra_in_target == ["O105"]
assert amount_mismatches == [{
    "order_id": "O102",
    "source_amount": 950,
    "target_amount": 900
}]


# COMMAND ----------

assert condition
