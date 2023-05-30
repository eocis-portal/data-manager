# -*- coding: utf-8 -*-

#    EOCIS data-manager
#    Copyright (C) 2020-2023  National Centre for Earth Observation (NCEO)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

class Transaction:

    def __init__(self, store):
        self.store = store
        self.conn = store.open_connection()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is None:
            self.commit()
            return True
        else:
            self.rollback()
            return False

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def collect_results(self, curs):
        rows = []
        column_names = [column[0] for column in curs.description]
        for row in curs.fetchall():
            rows.append({v1: v2 for (v1, v2) in zip(column_names, row)})
        return rows
