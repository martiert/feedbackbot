import json
import logging

import pymongo
import aiohttp
import asyncio

from spark import Server

help_message = '''
 * help - Show this message
 * add customer <customer>: <emails> - Add customer, or emails to existing customer.
    1. customer name can not contain ':' characters
    2. customer name can contain spaces
    3. emails are split with spaces
 * remove customer <customer>: <emails> - Remove emails from existing customer entry
 * remove customer <customer>: all - Completely remove customer
 * list customers - List all your customers
 * list emails <customer> - List emails for given customer
 * give customer <receiver> <customer> - Give customer to receiver
'''

admin_help = '''
 * add contact <email> - Add contact person
 * remove contact <email> - Remove contact person, along with his customers
 * add admin <email> - Create admin, or give admin privileges to existing contact person
 * remove admin <email> - Remove admin privileges from contact person
 * steal customer <from> <customer> - Steal customer from contact person
'''



class Feedback:
    def __init__(self, config):
        self._states = {}
        self._next_id = {}
        self._setup_server(config)
        client = pymongo.MongoClient('mongodb://127.0.0.1')
        collection = client[config['database']]
        self._db = collection['contacts']
        self._customers = collection['customers']

    async def answer(self, api, message):
        pass

    async def steal_customer(self, api, message):
        loop = asyncio.get_event_loop()
        contact = self._db.find_one({'_id': message.personEmail})
        if not contact:
            return

        data = message.text.replace('steal customer', '').strip().split(' ')
        victim = data[0].strip()
        customer = ' '.join(data[1:]).strip()

        to_give = self._db.find_one({'_id': victim})
        if not to_give:
            await loop.run_in_executor(
                None,
                api.messages.create,
                None,
                None,
                message.personEmail,
                '{} does not exist'.format(victim))
            return

        if not customer in to_give.get('customers', {}):
            await loop.run_in_executor(
                None,
                api.messages.create,
                None,
                None,
                message.personEmail,
                'No customer named {} exists for {}'.format(customer, victim))
            return

        self._add_customer(message.personEmail, customer, to_give['customers'].get(customer, []))
        self._remove_customer(victim, customer, 'all')
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            'Stole customer {} from {}'.format(customer, victim))

    async def give_customer(self, api, message):
        loop = asyncio.get_event_loop()
        contact = self._db.find_one({'_id': message.personEmail})
        if not contact:
            return

        data = message.text.replace('give customer', '').strip().split(' ')
        receiver = data[0].strip()
        customer = ' '.join(data[1:]).strip()

        if not customer in contact.get('customers', {}):
            await loop.run_in_executor(
                None,
                api.messages.create,
                None,
                None,
                message.personEmail,
                'No customer named {} exists for you'.format(customer))
            return

        to_give = self._db.find_one({'_id': receiver})
        if not to_give:
            await loop.run_in_executor(
                None,
                api.messages.create,
                None,
                None,
                message.personEmail,
                'Contact person {} does not exist'.format(receiver))
            return

        self._add_customer(receiver, customer, contact['customers'].get(customer, []))
        self._remove_customer(message.personEmail, customer, 'all')
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            'Moved customer {} to {}'.format(customer, receiver))

    async def list_customers(self, api, message):
        contact = self._db.find_one({'_id': message.personEmail})
        if not contact:
            return

        result = 'Your customers:'
        for key in contact.get('customers', {}).keys():
            result += '\n * {}'.format(key)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            None,
            result)

    async def list_emails(self, api, message):
        loop = asyncio.get_event_loop()
        contact = self._db.find_one({'_id': message.personEmail})
        if not contact:
            return

        customer = message.text.replace('list emails', '').strip()
        result = 'Emails registered for customer {}:'.format(customer)

        customers = contact.get('customers', {})
        if not customer in customers:
            await loop.run_in_executor(
                None,
                api.messages.create,
                None,
                None,
                message.personEmail,
                'You have no customers namded {}'.format(customer))
            return

        for email in customers.get(customer, []):
            result += '\n * {}'.format(email)

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            None,
            result)

    async def add_customer(self, api, message):
        loop = asyncio.get_event_loop()
        is_contact = self._db.find_one({'_id': message.personEmail})
        if not is_contact:
            return

        data = message.text.replace('add customer', '').strip().split(':')

        if not len(data) > 1:
            await loop.run_in_executor(
                None,
                api.messages.create,
                None,
                None,
                message.personEmail,
                'Wrong format. Expected <customer name>: mail1 mail2 mail3')
            return

        customer = data[0]
        emails = set(''.join(data[1:]).strip().split(' '))
        if '' in emails:
            emails.remove('')

        to_add = []
        for email in emails:
            if not self._customers.find_one({'_id': email}):
                self._customers.insert({'_id': email})
                to_add.append(email)
            else:
                await loop.run_in_executor(
                    None,
                    api.messages.create,
                    None,
                    None,
                    message.personEmail,
                    'Not adding {} as email is already in use'.format(email))

        self._add_customer(message.personEmail, customer, to_add)

        customers = self._db.find_one({'_id': message.personEmail})['customers']
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            '{} are now registered with {}'.format(customer, customers[customer]))

    async def remove_customer(self, api, message):
        loop = asyncio.get_event_loop()
        is_contact = self._db.find_one({'_id': message.personEmail})
        if not is_contact:
            return

        data = message.text.replace('remove customer', '').strip().split(':')
        customer = data[0]

        content = ''.join(data[1:]).strip()
        removed = self._remove_customer(message.personEmail, customer, content)
        customers = self._db.find_one({'_id': message.personEmail}).get('customers')

        result = '{} is completely removed'.format(customer)
        if customer in customers:
            result = '{} are now registered with {}'.format(customer, customers[customer])

        for email in removed:
            self._customers.remove({'_id': email})

        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            result)

    async def add_admin(self, api, message):
        is_admin = self._db.find_one({'_id': message.personEmail, 'admin': True})
        if not is_admin:
            return

        email = message.text.replace('add admin', '').strip()
        response = '{} is already an admin'
        entry = self._db.find_one({'_id': email})

        if not entry:
            self._db.insert({'_id': email, 'admin': True, 'customers': {}})
            response = 'created {} as admin'
        elif not entry.get('admin', False):
            self._db.update({'_id': email}, {'$set': {'admin': True}})
            response = 'Gave admin privileges to {}'

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            response.format(email))

    async def remove_admin(self, api, message):
        is_admin = self._db.find_one({'_id': message.personEmail, 'admin': True})
        if not is_admin:
            return

        email = message.text.replace('remove admin', '').strip()
        entry = self._db.find_one({'_id': email, 'admin': True})

        if not entry:
            response = 'Could not find admin {}'.format(email)
        else:
            self._db.update({'_id': email}, {'$set': {'admin': False}})
            response = 'Removed admin privileges from {}'

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            response.format(email))

    async def add_contact(self, api, message):
        is_admin = self._db.find_one({'_id': message.personEmail, 'admin': True})
        if not is_admin:
            return

        email = message.text.replace('add contact', '').strip()
        entry = self._db.find_one({'_id': email})
        response = 'Contact {} already exists'

        if not entry:
            self._db.insert({'_id': email, 'admin': False, 'customers': {}})
            response = 'Added {} as contact'

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            response.format(email))

    async def remove_contact(self, api, message):
        is_admin = self._db.find_one({'_id': message.personEmail, 'admin': True})
        if not is_admin:
            return

        email = message.text.replace('remove contact', '').strip()
        entry = self._db.find_one({'_id': email})
        response = 'Contact {} does not exist'

        if entry:
            self._db.remove({'_id': email})
            response = 'Removed {} from contacts'

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            None,
            message.personEmail,
            response.format(email))

    async def help(self, api, message):
        is_contact = self._db.find_one({'_id': message.personEmail})
        if not is_contact:
            return

        response = help_message
        if is_contact.get('admin', False):
            response += admin_help

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            api.messages.create,
            None,
            message.personId,
            None,
            None,
            response)

    def _add_customer(self, contact, customer, emails):
        customers = self._db.find_one({'_id': contact}).get('customers', {})
        existing = set(customers.get(customer, []))

        existing.update(set(emails))
        customers[customer] = list(existing)
        self._db.update({'_id': contact}, {'$set': {'customers': customers}});

    def _remove_customer(self, contact, customer, to_remove):
        customers = self._db.find_one({'_id': contact}).get('customers', {})
        if customer not in customers:
            return []

        emails = customers[customer]
        if to_remove == 'all':
            del customers[customer]
        else:
            emails = to_remove.split(' ')
            existing = set(customers.get(customer, []))
            existing = existing - set(emails)
            customers[customer] = list(existing)

        self._db.update({'_id': contact}, {'$set': {'customers': customers}});
        return emails

    def _setup_server(self, config):
        loop = asyncio.get_event_loop()
        self._server = Server(
            config['bot'],
            loop
        )
        self._server.default_message(self.answer)
        self._server.listen('^help$', self.help)
        self._server.listen('^list customers$', self.list_customers)
        self._server.listen('^list emails', self.list_emails)
        self._server.listen('^give customer', self.give_customer)
        self._server.listen('^steal customer', self.steal_customer)
        self._server.listen('^add admin', self.add_admin)
        self._server.listen('^add contact', self.add_contact)
        self._server.listen('^add customer', self.add_customer)
        self._server.listen('^remove admin', self.remove_admin)
        self._server.listen('^remove contact', self.remove_contact)
        self._server.listen('^remove customer', self.remove_customer)

        loop.run_until_complete(self._server.setup())

    def run(self):
        loop = asyncio.get_event_loop()
        print('======== Bot Ready ========')
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass
        except:
            print(sys.exc_info())
        finally:
            loop.run_until_complete(self._server.cleanup())
