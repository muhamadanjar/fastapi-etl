from uuid import UUID
from datetime import datetime
from typing import Optional, List
from sqlmodel import Session, select
from sqlalchemy import update as sa_update
from app.infrastructure.db.models.raw_data.upload_session import UploadSession, UploadSessionStatus
from app.infrastructure.db.repositories.base import BaseRepository


class UploadSessionRepository(BaseRepository[UploadSession]):
    """Repository for UploadSession database operations"""

    def __init__(self, session: Session):
        super().__init__(UploadSession, session)

    async def update_chunk_map(
        self,
        session_id: UUID,
        chunk_index: int,
        received_bytes: int,
        uploaded_chunks: int
    ) -> Optional[UploadSession]:
        """Update chunk_map, received_bytes, and uploaded_chunks"""
        session = await self.get(session_id)
        if not session:
            return None

        chunk_map = dict(session.chunk_map or {})
        chunk_map[str(chunk_index)] = True

        new_status = session.status
        if session.status == UploadSessionStatus.PENDING:
            new_status = UploadSessionStatus.UPLOADING

        stmt = sa_update(UploadSession).where(
            UploadSession.id == session_id
        ).values(
            chunk_map=chunk_map,
            received_bytes=received_bytes,
            uploaded_chunks=uploaded_chunks,
            status=new_status
        )

        self.session.execute(stmt)
        self.session.flush()

        return await self.get(session_id)

    async def mark_completed(self, session_id: UUID, file_registry_id: UUID) -> Optional[UploadSession]:
        """Mark session as completed and link to FileRegistry"""
        session = await self.get(session_id)
        if not session:
            return None

        session.status = UploadSessionStatus.COMPLETED
        session.file_registry_id = file_registry_id

        return await self.update(id=session_id, obj_in=session)

    async def mark_expired(self, session_id: UUID) -> Optional[UploadSession]:
        """Mark session as expired"""
        session = await self.get(session_id)
        if not session:
            return None

        session.status = UploadSessionStatus.EXPIRED
        return await self.update(id=session_id, obj_in=session)

    async def get_expired_sessions(self) -> List[UploadSession]:
        """Get all expired sessions"""
        now = datetime.utcnow()
        statement = select(UploadSession).where(
            (UploadSession.expires_at < now) &
            (UploadSession.status != UploadSessionStatus.COMPLETED)
        )
        return self.session.exec(statement).all()

    async def cleanup_expired_sessions(self) -> int:
        """Mark expired sessions and return count"""
        expired = await self.get_expired_sessions()
        for session in expired:
            session.status = UploadSessionStatus.EXPIRED
            self.session.add(session)

        self.session.commit()
        return len(expired)
