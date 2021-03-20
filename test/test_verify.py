# Verify test results in test.csv make sense
# To do this test, run a j2r batch with -i test.json -e ./test_task
# Then j2r -a dump-experiments -n TEST_NAME > test.csv
# Then run this verifier
import csv

# List of test arguments
test_args = ['test-int', 'test-str1', 'test-str2']

with open('test.csv', 'rb') as csvf:
    # Read csv
    res = csv.DictReader(csvf)
    verified = True
    # Check each row
    for r in res:
        # Check each test arg
        for arg in test_args:
            if r[arg] != r['o-%s'%arg]:
                verified = False
                print("Test failure")
                print(r[arg], r['o-%s'%arg])

    # If verified is true we succeeded
    if verified:
        print("Test passed!")
