#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы main_runner.py
"""

import subprocess
import sys
import time


def test_runner():
    """Тестирование различных режимов работы."""

    tests = [
        {
            'name': 'Dry run daily',
            'command': [sys.executable, 'scripts/main_runner.py', '--mode', 'daily', '--dry-run'],
            'expected_code': 0
        },
        {
            'name': 'Dry run weekly',
            'command': [sys.executable, 'scripts/main_runner.py', '--mode', 'weekly', '--dry-run'],
            'expected_code': 0
        },
        {
            'name': 'Manual task with limit',
            'command': [sys.executable, 'scripts/main_runner.py', '--mode', 'manual',
                        '--task', 'discover_films', '--limit', '5'],
            'expected_code': 0
        },
        {
            'name': 'Invalid mode',
            'command': [sys.executable, 'scripts/main_runner.py', '--mode', 'invalid'],
            'expected_code': 1
        }
    ]

    print("🧪 Начало тестирования main_runner.py")
    print("=" * 50)

    results = []

    for test in tests:
        print(f"\n🔍 Тест: {test['name']}")
        print(f"   Команда: {' '.join(test['command'][1:])}")

        start_time = time.time()
        try:
            result = subprocess.run(
                test['command'],
                capture_output=True,
                text=True,
                timeout=30
            )
            duration = time.time() - start_time

            if result.returncode == test['expected_code']:
                print(f"   ✅ Успех за {duration:.2f} сек")
                results.append(True)
            else:
                print(f"   ❌ Ошибка: код {result.returncode} (ожидался {test['expected_code']})")
                print(f"   stdout: {result.stdout[:200]}...")
                print(f"   stderr: {result.stderr[:200]}...")
                results.append(False)

        except subprocess.TimeoutExpired:
            print(f"   ⏱️  Таймаут (30 сек)")
            results.append(False)
        except Exception as e:
            print(f"   ❌ Исключение: {e}")
            results.append(False)

    print("\n" + "=" * 50)
    success_count = sum(results)
    total_count = len(results)

    if success_count == total_count:
        print(f"🎉 Все тесты пройдены успешно! ({success_count}/{total_count})")
        return 0
    else:
        print(f"⚠️  Пройдено {success_count} из {total_count} тестов")
        return 1


if __name__ == "__main__":
    sys.exit(test_runner())