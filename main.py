from data_generator.data_generator import TransactionDataGenerator
import json
generator = TransactionDataGenerator().generate_visa_transaction()
data = json.dumps(generator,indent=4)
print(data)
