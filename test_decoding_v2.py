from urllib.parse import unquote

def _decode_value(value):
    if not value:
        return ""
    try:
        value = unquote(value).strip()
        # Remove surrounding quotes if present (some browsers/servers add them)
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        return value.strip()
    except Exception:
        return str(value).strip()

def test(val):
    print(f"Input: '{val}' -> Output: '{_decode_value(val)}'")

print("--- Testing Email Decoding ---")
test("rohit.bairwa%40lumberfi.com")
test('"rohit.bairwa%40lumberfi.com"')
test("rohit.bairwa@lumberfi.com")

print("\n--- Testing Project Key Decoding ---")
test("TIM")
test('"TIM"')
test("TIM%20")
test('"TIM%20"')

print("\n--- Testing Double Encoding Scenarios (Just in case) ---")
test("rohit.bairwa%2540lumberfi.com") # %25 is %
