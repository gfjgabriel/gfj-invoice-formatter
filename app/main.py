from fastapi import FastAPI, UploadFile, Depends
from fastapi.responses import StreamingResponse

from app.security import check_api_key
from app.service import filter_transactions, generate_csv

app = FastAPI()

@app.post("/process")
async def process(
    file: UploadFile,
    computed_file: UploadFile | None = None,
    _: None = Depends(check_api_key),
):
    transactions = filter_transactions(file, computed_file)
    output = generate_csv(transactions)
    return StreamingResponse(
        output,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=new_invoice.csv"}
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000)
