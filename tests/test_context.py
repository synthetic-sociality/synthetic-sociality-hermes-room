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

    def test_later_message_receives_library_catalog_without_document_text(self):
        current = {"id": "later", "type": "message.posted", "payload": {"body": "Read the document"}}
        library = [{
            "artifactId": "artifact-1", "visibility": "room_shared", "title": "Agenda",
            "currentVersion": {
                "versionId": "version-1", "name": "agenda.docx",
                "mediaType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sha256": "b" * 64, "extractionStatus": "ready",
                "extractedText": "Collective agenda text",
            },
        }, {
            "artifactId": "artifact-private", "visibility": "private", "title": "Hidden",
            "currentVersion": {"versionId": "version-p", "name": "hidden.pdf", "extractedText": "secret"},
        }]
        rendered = context.source_artifact_context(
            current, [], lambda _artifact_id: self.fail("catalog listing must not fetch any artifact"), library,
        )
        self.assertIn("catalog only", rendered)
        self.assertIn("agenda.docx", rendered)
        self.assertIn("artifact-1 / version-1", rendered)
        self.assertIn("b" * 64, rendered)
        self.assertNotIn("Collective agenda text", rendered)
        self.assertNotIn("hidden.pdf", rendered)
        self.assertNotIn("secret", rendered)
        self.assertLessEqual(len(rendered), context.LIBRARY_CATALOG_CHARACTER_LIMIT)

    def test_pending_library_version_is_listed_as_pending_without_backfill_read(self):
        current = {"id": "later", "type": "message.posted", "payload": {"body": "Read it"}}
        library = [{
            "artifactId": "artifact-1", "visibility": "room_shared", "title": "Agenda",
            "currentVersion": {
                "versionId": "version-1", "name": "agenda.docx",
                "mediaType": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "sha256": "c" * 64, "extractionStatus": "pending", "extractedText": "",
            },
        }]
        rendered = context.source_artifact_context(
            current, [], lambda _artifact_id: self.fail("catalog listing must not read pending versions"), library,
        )
        self.assertIn("extraction pending", rendered)

    def test_bound_attachment_keeps_full_text_while_library_stays_catalog(self):
        source = {
            "id": "human-message", "seq": 10, "type": "message.posted",
            "payload": {"body": "Read this", "attachments": [{
                "artifactId": "artifact-1", "versionId": "version-1", "name": "bound.pdf",
                "mediaType": "application/pdf", "sha256": "d" * 64,
            }]},
        }
        library = [
            {"artifactId": "artifact-1", "visibility": "room_shared", "title": "Bound",
             "currentVersion": {"versionId": "version-1", "name": "bound.pdf", "extractionStatus": "ready", "extractedText": "IGNORED"}},
            {"artifactId": "artifact-2", "visibility": "room_shared", "title": "Other",
             "currentVersion": {"versionId": "version-9", "name": "other.pdf", "extractionStatus": "ready", "extractedText": "Other full text"}},
        ]
        rendered = context.source_artifact_context(source, [], lambda _artifact_id: {
            "artifactId": "artifact-1",
            "currentVersion": {"versionId": "version-1", "extractionStatus": "ready", "extractedText": "Exact bound text"},
        }, library)
        self.assertIn("Exact bound text", rendered)
        self.assertIn("other.pdf", rendered)
        self.assertNotIn("Other full text", rendered)
        self.assertNotIn("- bound.pdf", rendered)  # the bound version is not repeated as a catalog line

    def test_b13_library_growth_and_wave_count_do_not_grow_the_document_context(self):
        # Two synthetic documents of 22,500 characters each (45k total, the
        # Kabeltracker shape) and twelve human waves. Before this change every
        # wave carried min(64k, 45k) = 45k characters of library text.
        documents = [
            {"artifactId": f"artifact-{index}", "visibility": "room_shared", "title": f"Doc {index}",
             "currentVersion": {"versionId": f"v{index}", "name": f"doc-{index}.pdf", "mediaType": "application/pdf",
                                "sha256": chr(ord("a") + index) * 64, "extractionStatus": "ready",
                                "extractedText": ("Lorem ipsum " * 2_050)[:22_500]}}
            for index in range(2)
        ]
        before_per_wave = min(context.ARTIFACT_CONTEXT_CHARACTER_LIMIT, sum(len(d["currentVersion"]["extractedText"]) for d in documents))
        sizes = []
        for wave in range(12):
            current = {"id": f"wave-{wave}", "type": "message.posted", "payload": {"body": f"Question {wave}"}}
            rendered = context.source_artifact_context(current, [], lambda _id: self.fail("no fetch"), documents)
            sizes.append(len(rendered))
            self.assertNotIn("Lorem ipsum", rendered)
        self.assertEqual(len(set(sizes)), 1, sizes)
        self.assertLess(sizes[0], 1_000)
        self.assertGreater(before_per_wave, 40_000)
        print(f"\nB13 measurement (Hermes document context per wave): before={before_per_wave} chars, after={sizes[0]} chars, waves=12")

    def test_discussion_handover_context_renders_bounded_sections_and_sources(self):
        start_event = {
            "id": "start-2", "seq": 41, "type": "discussion.started",
            "payload": {
                "epoch": {"id": "epoch-2", "ordinal": 2, "startsAtSeq": 41, "topic": {"title": "Continue"}, "status": "active"},
                "handover": {
                    "sourceEpochId": "epoch-1", "sourceEpochOrdinal": 1, "coveredThroughSeq": 40, "revision": 2,
                    "reviewStatus": "human_reviewed", "digest": "f" * 64, "submittedByMembershipId": "owner-1",
                    "sections": {
                        "goal": "Reduce token consumption.",
                        "decisions": [{"statement": "Fresh session per discussion.", "rationale": "History accumulated."}],
                        "dissent": ["One agent prefers a lossless transcript."],
                        "openQuestions": ["Which runtime hook reports usage?"],
                        "results": ["Kabeltracker growth documented."],
                        "nextSteps": ["Ship the connector change."],
                    },
                    "sourceRefs": [
                        {"kind": "event", "eventId": "evt-40", "seq": 40, "note": "decision message"},
                        {"kind": "artifact_version", "artifactId": "art-1", "versionId": "ver-1", "sha256": "e" * 64},
                    ],
                },
            },
        }
        rendered = context.discussion_handover_context(start_event)
        for expected in (
            "Reviewed handover from discussion #1", "covered through canonical seq 40", "revision 2",
            "Goal: Reduce token consumption.", "- Fresh session per discussion. — because History accumulated.",
            "Dissent:", "Open questions:", "Results:", "Next steps:", "event evt-40 (seq 40) — decision message",
            "artifact art-1 / ver-1", "Derived summary, not a source",
        ):
            self.assertIn(expected, rendered)
        self.assertEqual(context.discussion_handover_context({"payload": {"epoch": {}}}), "")
        self.assertEqual(context.discussion_handover_context(None), "")
        start_event["payload"]["handover"]["sections"]["results"] = ["x" * 2_000] * 10
        self.assertLessEqual(len(context.discussion_handover_context(start_event)), context.HANDOVER_CONTEXT_CHARACTER_LIMIT)

    def test_epoch_start_event_reads_exactly_the_epoch_opening_event(self):
        pages = {40: {"events": [
            {"id": "start-2", "seq": 41, "type": "discussion.started", "payload": {"epoch": {"id": "epoch-2"}}},
            {"id": "m-42", "seq": 42, "type": "message.posted", "payload": {"body": "hi"}},
        ]}}
        calls = []

        def fetch(after):
            calls.append(after)
            return pages.get(after, {"events": []})

        self.assertEqual(context.epoch_start_event(fetch, 41)["id"], "start-2")
        self.assertEqual(calls, [40])
        self.assertIsNone(context.epoch_start_event(fetch, 7))
        self.assertIsNone(context.epoch_start_event(fetch, 0))

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
