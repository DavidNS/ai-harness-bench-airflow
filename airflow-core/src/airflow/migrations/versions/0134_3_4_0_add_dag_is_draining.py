#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Add is_draining to dag and an index on backfill (dag_id, completed_at).

Revision ID: c4d9e7a3b165
Revises: f8c2a1d94e03
Create Date: 2026-07-20 02:00:00.000000

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c4d9e7a3b165"
down_revision = "f8c2a1d94e03"
branch_labels = None
depends_on = None
airflow_version = "3.4.0"


def upgrade():
    """Add dag.is_draining and backfill (dag_id, completed_at) index."""
    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column("is_draining", sa.Boolean(), server_default="0", nullable=False),
        )
        batch_op.create_index("idx_dag_is_draining", ["is_draining"], unique=False)
        batch_op.create_check_constraint(
            batch_op.f("ck_dag_not_paused_and_draining"), "NOT (is_paused AND is_draining)"
        )

    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.create_index("idx_backfill_dag_id_completed_at", ["dag_id", "completed_at"], unique=False)


def downgrade():
    """Remove dag.is_draining and backfill (dag_id, completed_at) index."""
    with op.batch_alter_table("backfill", schema=None) as batch_op:
        batch_op.drop_index("idx_backfill_dag_id_completed_at")

    with op.batch_alter_table("dag", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("ck_dag_not_paused_and_draining"), type_="check")
        batch_op.drop_index("idx_dag_is_draining")
        batch_op.drop_column("is_draining")
