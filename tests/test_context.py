from __future__ import annotations

import unittest

from support import load_module


context = load_module("context")


class ArtifactContextTests(unittest.TestCase):
    def test_cycle_source_resolves_exact_room_shared_document_version(self):
        source = {
            "id": "human-message", "seq": 10, "type": "message.posted",
            "payload": {"body": "Read this", "attachments": [{
                "artifactId": "artifact-1", "versionId": "version-1",
                "name": "开幕式日程.docx",
                "mediaType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sha256": "a" * 64,
            }]},
        }
        ready = {
            "id": "ready-1", "seq": 12, "type": "discussion.cycle_attempt_ready",
            "payload": {"sourceEventId": "human-message"},
        }
        calls = []

        def fetch(artifact_id):
            calls.append(artifact_id)
            return {
                "artifactId": artifact_id,
                "currentVersion": {
                    "versionId": "version-2", "extractionStatus": "ready",
                    "extractedText": "wrong",
                },
                "versions": [
                    {"versionId": "version-2", "extractionStatus": "ready", "extractedText": "wrong"},
                    {"versionId": "version-1", "extractionStatus": "ready", "extractedText": "09:00 开幕式\n09:30 Keynote"},
                ],
            }

        rendered = context.source_artifact_context(ready, [source], fetch)
        self.assertEqual(calls, ["artifact-1"])
        self.assertIn("开幕式日程.docx", rendered)
        self.assertIn("09:00 开幕式", rendered)
        self.assertNotIn("wrong", rendered)
        self.assertIn("untrusted uploaded content", rendered)

    def test_later_message_discovers_authorized_room_library_without_reattachment(self):
        current = {"id": "later", "type": "message.posted", "payload": {"body": "Read the document"}}
        library = [{
            "artifactId": "artifact-1", "visibility": "room_shared", "title": "Agenda",
            "currentVersion": {
                "versionId": "version-1", "name": "agenda.docx",
                "mediaType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sha256": "b" * 64, "extractionStatus": "ready",
                "extractedText": "Collective agenda text",
            },
        }]
        rendered = context.source_artifact_context(
            current, [], lambda _artifact_id: self.fail("ready library item must not refetch"), library,
        )
        self.assertIn("agenda.docx", rendered)
        self.assertIn("Collective agenda text", rendered)

    def test_pending_library_version_uses_supported_exact_read_backfill(self):
        current = {"id": "later", "type": "message.posted", "payload": {"body": "Read it"}}
        library = [{
            "artifactId": "artifact-1", "visibility": "room_shared", "title": "Agenda",
            "currentVersion": {
                "versionId": "version-1", "name": "agenda.docx",
                "mediaType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sha256": "c" * 64, "extractionStatus": "pending", "extractedText": "",
            },
        }]
        calls = []

        def fetch(artifact_id):
            calls.append(artifact_id)
            return {
                "artifactId": artifact_id,
                "currentVersion": {
                    "versionId": "version-1", "name": "agenda.docx",
                    "extractionStatus": "ready", "extractedText": "Backfilled agenda text",
                },
            }

        rendered = context.source_artifact_context(current, [], fetch, library)
        self.assertEqual(calls, ["artifact-1"])
        self.assertIn("Extraction status: ready", rendered)
        self.assertIn("Backfilled agenda text", rendered)

    def test_document_context_is_bounded_and_never_downloads_raw_bytes(self):
        source = {
            "id": "human-message", "type": "message.posted",
            "payload": {"attachments": [{
                "artifactId": "artifact-1", "versionId": "version-1", "name": "agenda.docx",
            }]},
        }
        rendered = context.source_artifact_context(source, [], lambda _artifact_id: {
            "currentVersion": {"versionId": "version-1", "extractionStatus": "pending"},
        })
        self.assertIn("Extraction status: pending", rendered)
        self.assertIn("Content is not yet available", rendered)
        self.assertLessEqual(len(rendered), context.ARTIFACT_CONTEXT_CHARACTER_LIMIT)


if __name__ == "__main__":
    unittest.main()
