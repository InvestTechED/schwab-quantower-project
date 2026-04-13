# Deployment Notes

## Build Output

Build the bridge DLL in Visual Studio or via `dotnet build`.

Primary artifacts:

- `SchwabVendor.dll`
- `schwab.svg`

## Vendor Deploy Folder

Copy the built files to:

`D:\Quantower\TradingPlatform\v1.146.4\bin\Vendors\SchwabVendor`

If the folder does not exist, create it.

## Runtime Requirement

The local Schwab backend must be running on:

`http://127.0.0.1:8000`
