from hfa.enrichment import EnrichmentStep, run_enrichment_pipeline
from hfa.provenance import ProvenanceEntry, compute_input_hash
from hfa.vault import read_note, write_card


class DescriptionStep(EnrichmentStep):
    def run(self, card, body, vault_path):
        input_hash = compute_input_hash({"card": card.model_dump(mode="python"), "body": body})
        return {
            "description": (
                "generated description",
                ProvenanceEntry(
                    source=self.name,
                    date="2026-03-06",
                    method=self.method,
                    model="mock-v1",
                    enrichment_version=self.version,
                    input_hash=input_hash,
                ),
            )
        }


def test_run_enrichment_pipeline_updates_card(tmp_vault, sample_person_card, sample_person_provenance):
    write_card(tmp_vault, "People/jane-smith.md", sample_person_card, provenance=sample_person_provenance)
    summary = run_enrichment_pipeline(
        str(tmp_vault),
        [DescriptionStep(name="description_gen", version=1, target_fields=["description"], method="llm")],
    )
    frontmatter, _, provenance = read_note(tmp_vault, "People/jane-smith.md")
    assert frontmatter["description"] == "generated description"
    assert provenance["description"].method == "llm"
    assert summary["description_gen"]["processed"] == 1
