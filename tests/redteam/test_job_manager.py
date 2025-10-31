"""Tests for red team job manager."""

import pytest
from src.core.redteam.job_manager import RedTeamJob, RedTeamJobManager
from src.core.redteam.llm_interface import LLMConfig
from src.core.redteam.schemas import (
    RedTeamJobStatus,
    RedTeamPrompt,
    StaticRedTeamDataset,
    StaticRedTeamJobRequest,
)


@pytest.fixture
def sample_request():
    """Create a sample job request."""
    dataset = StaticRedTeamDataset(
        dataset_id="test",
        name="Test",
        description="Test dataset",
        version="1.0",
        prompts=[
            RedTeamPrompt(
                prompt_id="p1",
                prompt="Test prompt",
                category="jailbreak",
                expected_behavior="Refuse",
                tags=[]
            )
        ]
    )
    
    return StaticRedTeamJobRequest(
        target_model=LLMConfig(
            model_type="openai",
            model_name="gpt-3.5-turbo",
            api_key="test-key"
        ),
        custom_dataset=dataset,
        judge_model=LLMConfig(
            model_type="openai",
            model_name="gpt-4",
            api_key="test-key"
        )
    )


def test_job_creation():
    """Test creating a new job."""
    manager = RedTeamJobManager()
    
    request = StaticRedTeamJobRequest(
        target_model=LLMConfig(
            model_type="openai",
            model_name="gpt-3.5-turbo"
        ),
        dataset_id="sample-jailbreaks-v1",
        judge_model=LLMConfig(
            model_type="openai",
            model_name="gpt-4"
        )
    )
    
    job_id = manager.create_job(request)
    
    assert job_id is not None
    assert job_id in manager.jobs
    
    job = manager.get_job(job_id)
    assert job.status == RedTeamJobStatus.QUEUED
    assert job.progress == 0.0


def test_job_summary(sample_request):
    """Test getting job summary."""
    manager = RedTeamJobManager()
    job_id = manager.create_job(sample_request)
    
    summary = manager.get_job_summary(job_id)
    
    assert summary is not None
    assert summary.job_id == job_id
    assert summary.job_type == "static"
    assert summary.status == RedTeamJobStatus.QUEUED
    assert summary.progress == 0.0
    assert summary.preliminary_stats == {}


def test_job_update_progress():
    """Test updating job progress."""
    job_id = "test-job"
    request = StaticRedTeamJobRequest(
        target_model=LLMConfig(
            model_type="openai",
            model_name="gpt-3.5-turbo"
        ),
        dataset_id="test",
        judge_model=LLMConfig(
            model_type="openai",
            model_name="gpt-4"
        )
    )
    
    job = RedTeamJob(job_id, request)
    
    # Initially at 0%
    assert job.progress == 0.0
    
    # Update to 50%
    job.update_progress(5, 10)
    assert job.progress == 50.0
    
    # Update to 100%
    job.update_progress(10, 10)
    assert job.progress == 100.0


def test_list_jobs(sample_request):
    """Test listing all jobs."""
    manager = RedTeamJobManager()
    
    # Create multiple jobs
    job_id1 = manager.create_job(sample_request)
    job_id2 = manager.create_job(sample_request)
    
    jobs = manager.list_jobs()
    
    assert len(jobs) >= 2
    job_ids = [j.job_id for j in jobs]
    assert job_id1 in job_ids
    assert job_id2 in job_ids


def test_delete_job(sample_request):
    """Test deleting a job."""
    manager = RedTeamJobManager()
    job_id = manager.create_job(sample_request)
    
    # Verify job exists
    assert manager.get_job(job_id) is not None
    
    # Delete job
    success = manager.delete_job(job_id)
    assert success is True
    
    # Verify job is gone
    assert manager.get_job(job_id) is None

