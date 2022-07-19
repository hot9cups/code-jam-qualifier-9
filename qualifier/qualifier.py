import typing
from dataclasses import dataclass
from collections import defaultdict, deque


@dataclass(frozen=True)
class Request:
    scope: typing.Mapping[str, typing.Any]

    receive: typing.Callable[[], typing.Awaitable[object]]
    send: typing.Callable[[object], typing.Awaitable[None]]


class RestaurantManager:
    def __init__(self):
        """Instantiate the restaurant manager.

        This is called at the start of each day before any staff get on
        duty or any orders come in. You should do any setup necessary
        to get the system working before the day starts here; we have
        already defined a staff dictionary.
        """
        self.staff = {}
        self.speciality_to_ids = defaultdict(deque)

    def find_staff_member(self, speciality: str):
        """Return on-duty staff member with specified speciality.
        This works in a Round-Robin fashion.

        """
        speciality_staff_members = self.speciality_to_ids[speciality]
        while True:
            member = speciality_staff_members.popleft()
            if member in self.staff.keys():
                speciality_staff_members.append(member)
                return self.staff[member]

    async def __call__(self, request: Request):
        """Handle a request received.

        This is called for each request received by your application.
        In here is where most of the code for your system should go.

        :param request: request object
            Request object containing information about the sent
            request to your application.
        """
        json_data = request.scope
        request_type = json_data["type"]

        if request_type == "staff.onduty":
            self.staff[json_data["id"]] = request
            for item in json_data["speciality"]:
                self.speciality_to_ids[item].append(json_data["id"])

        elif request_type == "staff.offduty":
            del self.staff[json_data["id"]]

        elif request_type == "order":
            speciality = json_data["speciality"]

            staff_member = self.find_staff_member(speciality)

            full_order = await request.receive()
            await staff_member.send(full_order)

            result = await staff_member.receive()
            await request.send(result)
