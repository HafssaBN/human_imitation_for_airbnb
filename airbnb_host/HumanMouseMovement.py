# HumanMouseMovement.py
from playwright.sync_api import Page
from math import sqrt, atan2, cos, sin
import time
import random

class HumanMouseMovement:
    def __init__(self, page: Page):
        self.page = page
        # Start near viewport center (more human than (0,0))
        vp = page.viewport_size or {"width": 1280, "height": 800}
        cx = int(vp["width"] * 0.5 + random.uniform(-20, 20))
        cy = int(vp["height"] * 0.5 + random.uniform(-20, 20))
        try:
            # Try to read any previously stored coords, fallback to center
            pos = page.evaluate("""() => ({ x: Math.round(window.mouseX || 0), y: Math.round(window.mouseY || 0) })""")
            x = int(pos.get("x", cx))
            y = int(pos.get("y", cy))
        except Exception:
            x, y = cx, cy
        self.previous_x = x
        self.previous_y = y
        # Move cursor once to set a known starting position
        try:
            self.page.mouse.move(self.previous_x, self.previous_y)
        except Exception:
            pass

    def _bezier_curve(self, start: tuple, end: tuple, control1: tuple, control2: tuple, t: float) -> tuple:
        x = (1 - t) ** 3 * start[0] + 3 * (1 - t) ** 2 * t * control1[0] + 3 * (1 - t) * t ** 2 * control2[0] + t ** 3 * end[0]
        y = (1 - t) ** 3 * start[1] + 3 * (1 - t) ** 2 * t * control1[1] + 3 * (1 - t) * t ** 2 * control2[1] + t ** 3 * end[1]
        return (x, y)

    def _generate_control_points(self, start: tuple, end: tuple) -> tuple:
        # Bias control points along the segment with a small perpendicular jitter
        dx, dy = end[0] - start[0], end[1] - start[1]
        dist = max(1.0, sqrt(dx*dx + dy*dy))
        ang = atan2(dy, dx)
        # Along-segment offsets
        a1 = random.uniform(0.2, 0.5) * dist
        a2 = random.uniform(0.5, 0.8) * dist
        # Perpendicular jitter
        jitter = max(6.0, min(dist * 0.15, 60.0))
        pjit1 = random.uniform(-jitter, jitter)
        pjit2 = random.uniform(-jitter, jitter)
        # Unit vectors
        ux, uy = cos(ang), sin(ang)
        px, py = -uy, ux
        control1 = (start[0] + ux * a1 + px * pjit1, start[1] + uy * a1 + py * pjit1)
        control2 = (start[0] + ux * a2 + px * pjit2, start[1] + uy * a2 + py * pjit2)
        return control1, control2

    def _calculate_steps(self, distance: float) -> int:
        # Scale steps with distance, clamp
        base = 45
        steps = int(base * (distance / 450.0))
        steps = max(25, min(steps, 280))
        # add a little randomness
        return max(20, steps + random.randint(-6, 6))

    def move_to(self, target_x: int, target_y: int, duration: float = None):
        start = (float(self.previous_x), float(self.previous_y))
        end = (float(target_x), float(target_y))
        dist = sqrt((end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2)

        if dist < 4:
            self.page.mouse.move(int(end[0]), int(end[1]))
            self.previous_x, self.previous_y = int(end[0]), int(end[1])
            return

        c1, c2 = self._generate_control_points(start, end)
        steps = self._calculate_steps(dist)
        if duration is None:
            # duration grows a bit with distance
            duration = random.uniform(0.35, 0.6) + min(0.8, dist / 900.0)
        delay = duration / steps

        # small random pauses to mimic hesitation
        pause_every = random.randint(12, 20)
        for i in range(steps + 1):
            t = i / steps
            # vary timing
            jitter = random.uniform(0.85, 1.2)
            dt = delay * jitter
            x, y = self._bezier_curve(start, end, c1, c2, t)
            # micro-jitter to avoid perfectly smooth curve
            x += random.uniform(-0.6, 0.6)
            y += random.uniform(-0.6, 0.6)
            self.page.mouse.move(int(x), int(y))
            self.previous_x, self.previous_y = x, y

            # occasional tiny pause
            if i % pause_every == 0 and 0 < i < steps:
                time.sleep(dt * random.uniform(1.5, 2.2))
            else:
                time.sleep(dt)

    def click(self, x: int = None, y: int = None, button: str = "left", delay: float = None):
        if x is not None and y is not None:
            self.move_to(x, y)
        if delay is None:
            delay = random.uniform(0.05, 0.14)
        self.page.mouse.down(button=button)
        time.sleep(delay)
        self.page.mouse.up(button=button)

    # --- optional helpers used by some flows ---

    def drag_to(self, x: int, y: int, hold_ms: int = None):
        """Human-like click-and-drag to an absolute point."""
        if hold_ms is None:
            hold_ms = random.randint(60, 140)
        self.page.mouse.move(int(self.previous_x), int(self.previous_y))
        self.page.mouse.down()
        time.sleep(hold_ms / 1000.0)
        self.move_to(x, y, duration=random.uniform(0.35, 0.9))
        time.sleep(random.uniform(0.03, 0.08))
        self.page.mouse.up()

    def drag_by(self, dx: int, dy: int):
        self.drag_to(int(self.previous_x + dx), int(self.previous_y + dy))

    def wiggle(self, radius: int = 6, times: int = 3):
        """Small random wiggle in place (sometimes helps trigger lazy UI)."""
        for _ in range(times):
            nx = int(self.previous_x + random.randint(-radius, radius))
            ny = int(self.previous_y + random.randint(-radius, radius))
            self.page.mouse.move(nx, ny)
            time.sleep(random.uniform(0.03, 0.07))

    def scroll_like_human(self, amount: int = None, steps: int = None):
        """Scroll with small bursts rather than a single large wheel event."""
        if amount is None:
            amount = random.randint(600, 1400)
        if steps is None:
            steps = random.randint(3, 6)
        chunk = int(amount / steps)
        for _ in range(steps):
            self.page.mouse.wheel(0, int(chunk * random.uniform(0.8, 1.25)))
            time.sleep(random.uniform(0.09, 0.18))
