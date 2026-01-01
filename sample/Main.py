# from DB import new_db, DB
# from typing import List, Dict, Any
# import os
# import traceback
#
#
# def insert(db: DB, data: Dict) -> str:
#     try:
#
#         db.put(data.keys(), data.values())
#
#     except Exception as e:
#         raise e
#
#     return 'SUCCESS: INSERTED!'
#
#
# def delete(db: DB, key: Any) -> str:
#     try:
#         db.delete(key)
#     except Exception as e:
#         raise e
#
#     return 'SUCCESS: DELETED!'
#
#
# def update(db: DB, data: Dict) -> str:
#     try:
#
#         db.put(data.keys(), data.values())
#
#     except Exception as e:
#         raise e
#
#     return 'SUCCESS: UPDATED!'
#
#
# def safe_exit(response: str) -> bool:
#     response = response.lower()
#     if response in ['n', 'exit', 'quit', 'no']:
#         return False
#     else:
#         return True
#
#
# def main(db: DB):
#     print('Starting')
#     flag = True
#     count = 0
#     while flag:
#         if count == 0:
#             initial_input = input('Please tell what do you want to do.'
#                                   'Select from the following options:'
#                                   '1. Insert'
#                                   '2. Delete'
#                                   '3. Update'
#                                   '4. Exit').lower()
#
#             key_input = input('Please provide key')
#             value_input = input('Please provide value')
#
#             if initial_input == 'insert':
#                 globals()[initial_input](db, {key_input: value_input})
#             elif initial_input == 'delete':
#                 print('No data available. Please insert some records first.')
#             elif initial_input == 'update':
#                 print('No data available. Please insert some records first.')
#             elif initial_input == 'exit':
#                 flag = globals()[initial_input]()
#
#         else:
#             next_input = input('do you want to proceed or quit. '
#                                'Please type yes/no'
#                                'with any of the following options:'
#                                '1. YES'
#                                '2. CONTINUE'
#                                '3. Y'
#                                '4. No'
#                                '5. QUIT'
#                                '6. EXIT'
#                                '7. N').lower()
#
#             if next_input in ['yes', 'y', 'continue']:
#                 initial_input = input('Please tell what do you want to do.'
#                                       'Select from the following options:'
#                                       '1. Insert'
#                                       '2. Delete'
#                                       '3. Update'
#                                       '4. Exit').lower()
#
#                 if initial_input == 'insert':
#                     key_input = input('Please provide key')
#                     value_input = input('Please provide value')
#                     globals()[initial_input](db, {key_input: value_input})
#
#                 elif initial_input == 'delete':
#                     key_input = input('Please provide key')
#                     globals()[initial_input](db, key_input)
#
#                 elif initial_input == 'update':
#                     key_input = input('Please provide key')
#                     value_input = input('Please provide value')
#                     globals()[initial_input](db, {key_input: value_input})
#
#                 elif initial_input == 'exit':
#                     flag = globals()[initial_input]()
#             else:
#                 print('Quiting!!!')
#                 flag = False
#
#
#
#
#
#
#
#     # try:
#     #     print("Inserting data in db")
#     #     db.put('a', 'apple')
#     #     db.put('b', 'banana')
#     #     db.put('c', 'cherry')
#     #     print("Add more data, overwrite some")
#     #     db.put('a', 'apricot')
#     #     db.put('d', 'date')
#     #     db.put('e', 'elderberry')
#     #
#     #     print('Add more data, delete some')
#     #     db.delete('b')
#     #     db.put('f', 'fig')
#     #     db.put('g', 'grape')
#     #
#     #     val, _ = db.get('a')
#     #     print(f"Get('a') = {val} (should be 'appricot')")
#     #     val_1, err = db.get('b')
#     #     if isinstance(err, LookupError):
#     #         print(f"Get('b') correctly returned LookupError")
#     #
#     #     val_2, _ = db.get('c')
#     #     print(f"Get('c') = {val_2} (should be 'cherry')")
#     #
#     # except Exception as e:
#     #     traceback.print_tb(e.__traceback__)
#     # finally:
#     #     db.close()
#
#
# if __name__ == '__main__':
#     db, err = new_db(3, 3)
#     if err:
#         print('Sorry something went wrong while initializing the db')
#         traceback.print_tb(err.__traceback__)
#     main(db)
#     db.close()
#
from DB import new_db, DB
from typing import Dict, Any
import traceback

# --- DB Operations ---

def insert(db: DB, data: Dict[Any, Any]) -> str:
    try:
        for k, v in data.items():
            db.put(k, v)
    except Exception as e:
        raise e
    return "SUCCESS: INSERTED!"

def update(db: DB, data: Dict[Any, Any]) -> str:
    try:
        for k, v in data.items():
            db.put(k, v)
    except Exception as e:
        raise e
    return "SUCCESS: UPDATED!"

def delete(db: DB, key: Any) -> str:
    try:
        db.delete(key)
    except Exception as e:
        raise e
    return "SUCCESS: DELETED!"

def get(db: DB, key: Any) -> str:
    try:
        value, err = db.get(key)
        if err:
            if isinstance(err, LookupError):
                return f"Key '{key}' not found."
            else:
                return f"Error retrieving key '{key}': {err}"
        return f"Value for key '{key}': {value}"
    except Exception as e:
        raise e

# --- Helper to perform actions dynamically ---
def perform_action(db: DB, action: str, first_insert_done: bool) -> bool:
    """
    Returns True to continue loop, False to exit.
    """
    action = action.lower()

    if action == "insert":
        key = input("Enter key: ")
        value = input("Enter value: ")
        result = insert(db, {key: value})
        print(result)
        return True

    elif action == "update":
        if not first_insert_done:
            print("No data available. Please insert some records first.")
            return True
        key = input("Enter key: ")
        value = input("Enter value: ")
        result = update(db, {key: value})
        print(result)
        return True

    elif action == "delete":
        if not first_insert_done:
            print("No data available. Please insert some records first.")
            return True
        key = input("Enter key: ")
        result = delete(db, key)
        print(result)
        return True

    elif action == "get":
        if not first_insert_done:
            print("No data available. Please insert some records first.")
            return True
        key = input("Enter key: ")
        result = get(db, key)
        print(result)
        return True

    elif action == "exit":
        print("Exiting...")
        return False

    else:
        print("Invalid action. Please choose insert, update, delete, get, or exit.")
        return True

# --- Main loop ---
def main(db: DB):
    print("DB CLI started. Type 'exit' to quit.")
    first_insert_done = False
    flag = True
    while flag:
        try:
            action = input("Choose action (insert/delete/update/get/exit): ").strip().lower()

            # Update first_insert_done if the action is insert or update
            if action in ["insert"]:
                first_insert_done = True

            flag = perform_action(db, action, first_insert_done)

        except Exception as e:
            print("Error occurred:", e)
            traceback.print_tb(e.__traceback__)

# --- Entry point ---
if __name__ == "__main__":
    db, err = new_db(5, 5)  # initialize DB
    if err:
        print("Failed to initialize DB:")
        traceback.print_tb(err.__traceback__)
    else:
        try:
            main(db)
        finally:
            db.close()
            print("DB closed.")
