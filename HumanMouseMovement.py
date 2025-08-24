from playwright.sync_api import Page
from math import sqrt
import time
import random


class HumanMouseMovement:
    def __init__(self, page: Page):
        self.page = page
        position = page.evaluate("""() => {
                return {
                    x: Math.round(window.mouseX || 0),
                    y: Math.round(window.mouseY || 0)
                }
            }""")

        x = int(position['x'])
        y = int(position['y'])
        self.previous_x = x
        self.previous_y = y

    def _bezier_curve(self, start: tuple, end: tuple, control1: tuple, control2: tuple, t: float) -> tuple:
        """Calculate point coordinates on a Bézier curve at time t."""
        x = (1 - t) ** 3 * start[0] + 3 * (1 - t) ** 2 * t * control1[0] + \
            3 * (1 - t) * t ** 2 * control2[0] + t ** 3 * end[0]
        y = (1 - t) ** 3 * start[1] + 3 * (1 - t) ** 2 * t * control1[1] + \
            3 * (1 - t) * t ** 2 * control2[1] + t ** 3 * end[1]
        return (x, y)

    def _generate_control_points(self, start: tuple, end: tuple) -> tuple:
        """Generate random control points for the Bézier curve."""
        # Calculate distance between start and end points
        distance = sqrt((end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2)

        # Generate random control points
        control1 = (
            start[0] + random.uniform(0.2, 0.8) * distance,
            start[1] + random.uniform(-0.5, 0.5) * distance
        )
        control2 = (
            end[0] - random.uniform(0.2, 0.8) * distance,
            end[1] + random.uniform(-0.5, 0.5) * distance
        )

        return control1, control2

    def _calculate_steps(self, distance: float) -> int:
        """Calculate number of steps based on distance."""
        base_steps = 50
        return max(int(base_steps * (distance / 500)), 25)

    def move_to(self, target_x: int, target_y: int, duration: float = None):
        """
        Move the mouse from current position to target position in a human-like manner.

        Args:
            target_x (int): Target X coordinate
            target_y (int): Target Y coordinate
            duration (float, optional): Duration of movement in seconds
        """
        # Get current mouse position
        start = (self.previous_x, self.previous_y)
        end = (target_x, target_y)

        # Calculate distance
        distance = sqrt((end[0] - start[0]) ** 2 + (end[1] - start[1]) ** 2)

        # If distance is too small, just move directly
        if distance < 5:
            self.page.mouse.move(target_x, target_y)
            self.previous_x, self.previous_y = target_x, target_y
            return

        # Generate control points for Bézier curve
        control1, control2 = self._generate_control_points(start, end)

        # Calculate number of steps and delay
        steps = self._calculate_steps(distance)
        if duration is None:
            duration = random.uniform(0.5, 1.5)
        delay = duration / steps

        # Move the mouse along the Bézier curve
        for i in range(steps + 1):
            t = i / steps

            # Add some randomness to the timing
            current_delay = delay * random.uniform(0.8, 1.2)

            # Calculate current point on curve
            current_x, current_y = self._bezier_curve(start, end, control1, control2, t)

            # Move mouse to current point
            self.page.mouse.move(current_x, current_y)

            # Update previous position
            self.previous_x, self.previous_y = current_x, current_y

            # Add delay
            time.sleep(current_delay)

    def click(self, x: int = None, y: int = None, button: str = "left", delay: float = None):
        """
        Move to coordinates (if provided) and perform a click.

        Args:
            x (int, optional): X coordinate to click
            y (int, optional): Y coordinate to click
            button (str, optional): Mouse button to click ("left" or "right")
            delay (float, optional): Delay between mouse down and up
        """
        if x is not None and y is not None:
            self.move_to(x, y)

        if delay is None:
            delay = random.uniform(0.05, 0.15)

        self.page.mouse.down(button=button)
        time.sleep(delay)
        self.page.mouse.up(button=button)