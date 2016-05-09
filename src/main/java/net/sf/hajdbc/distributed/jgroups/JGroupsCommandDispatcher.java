/*
 * HA-JDBC: High-Availability JDBC
 * Copyright (C) 2012  Paul Ferraro
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.sf.hajdbc.distributed.jgroups;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import net.sf.hajdbc.Messages;
import net.sf.hajdbc.distributed.Command;
import net.sf.hajdbc.distributed.CommandDispatcher;
import net.sf.hajdbc.distributed.CommandResponse;
import net.sf.hajdbc.distributed.Member;
import net.sf.hajdbc.distributed.MembershipListener;
import net.sf.hajdbc.distributed.Stateful;
import net.sf.hajdbc.logging.Level;
import net.sf.hajdbc.logging.Logger;
import net.sf.hajdbc.logging.LoggerFactory;
import net.sf.hajdbc.util.Objects;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.util.Rsp;

/**
 * A JGroups-based command dispatcher.
 * 
 * @author Paul Ferraro
 * @see org.jgroups.blocks.MessageDispatcher
 * @param <C> the execution context type
 */
public class JGroupsCommandDispatcher<C> implements RequestHandler, CommandDispatcher<C>, org.jgroups.MembershipListener, MessageListener
{
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	private final String id;
	private final long timeout;
	private final MessageDispatcher dispatcher;
	private final C context;
	private final AtomicReference<View> viewReference = new AtomicReference<View>();
	private final MembershipListener membershipListener;
	private final List<MembershipListener> userMembershipListeners = new ArrayList<MembershipListener>();
	private final Stateful stateful;
	
	/**
	 * Constructs a new ChannelCommandDispatcher.
	 * @param id the channel name
	 * @param channel a JGroups channel
	 * @param timeout the command timeout
	 * @param context the execution context
	 * @param stateful the state transfer handler
	 * @param membershipListener notified of membership changes
	 * @throws Exception if channel cannot be created
	 */
	public JGroupsCommandDispatcher(String id, Channel channel, long timeout, C context, Stateful stateful, MembershipListener membershipListener) throws Exception
	{
		this.id = id;
		this.context = context;
		this.stateful = stateful;
		this.membershipListener = membershipListener;
		
		this.dispatcher = new MessageDispatcher(channel, this, this, this);
		this.timeout = timeout;
	}

	public void addUserMembershipListener(MembershipListener userMembershipListener) {
		this.userMembershipListeners.add(userMembershipListener);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#start()
	 */
	@Override
	public void start() throws Exception
	{
		Channel channel = this.dispatcher.getChannel();
		
		channel.setDiscardOwnMessages(false);
		
		// Connect and fetch state
		channel.connect(this.id, null, 0);
	}

	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.Lifecycle#stop()
	 */
	@Override
	public void stop()
	{
		Channel channel = this.dispatcher.getChannel();
		
		if (channel.isOpen())
		{
			if (channel.isConnected())
			{
				channel.disconnect();
			}
		}
	}

	@Override
	protected void finalize()
	{
		this.dispatcher.getChannel().close();
	}

	@Override
	public <R> Map<Member, CommandResponse<R>> executeAll(Command<R, C> command, Member... excludedMembers) throws Exception
	{
		Message message = this.createMessage(null, command);
		RequestOptions options = this.createRequestOptions();
		
		if ((excludedMembers != null) && (excludedMembers.length > 0))
		{
			Address[] exclusions = new Address[excludedMembers.length];
			for (int i = 0; i < excludedMembers.length; ++i)
			{
				exclusions[i] = ((AddressMember) excludedMembers[i]).getAddress();
			}
			options.setExclusionList(exclusions);
		}

		Map<Address, Rsp<R>> responses = this.dispatcher.castMessage(null, message, options);
		
		Map<Member, CommandResponse<R>> results = new TreeMap<Member, CommandResponse<R>>();
		
		for (Map.Entry<Address, Rsp<R>> entry: responses.entrySet())
		{
			Rsp<R> response = entry.getValue();
			
			if (response.wasReceived() && !response.wasSuspected()) {
				results.put(new AddressMember(entry.getKey()), new RspCommandResponse<R>(response));
			}
		}
		
		return results;
	}
	
	@Override
	public <R> CommandResponse<R> execute(Command<R, C> command, Member member) throws Exception
	{
		Message message = this.createMessage(((AddressMember) member).getAddress(), command);
		// Use sendMessageWithFuture(...) instead of sendMessage(...) since we want to differentiate between sender exceptions and receiver exceptions
		Future<R> future = this.dispatcher.sendMessageWithFuture(message, this.createRequestOptions());
		try
		{
			return new SimpleCommandResponse<R>(future.get());
		}
		catch (InterruptedException e)
		{
			return new ExceptionCommandResponse<R>(new ExecutionException(e));
		}
		catch (ExecutionException e)
		{
			return new ExceptionCommandResponse<R>(e);
		}
	}
	
	private <R> Message createMessage(Address destination, Command<R, C> command)
	{
		return new Message(destination, this.getLocalAddress(), Objects.serialize(command));
	}
	
	private RequestOptions createRequestOptions()
	{
		return new RequestOptions(ResponseMode.GET_ALL, this.timeout, false, null, Message.Flag.DONT_BUNDLE, Message.Flag.OOB);
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.CommandDispatcher#getLocal()
	 */
	@Override
	public AddressMember getLocal()
	{
		return new AddressMember(this.getLocalAddress());
	}
	
	private Address getLocalAddress()
	{
		return this.dispatcher.getChannel().getAddress();
	}
	
	/**
	 * {@inheritDoc}
	 * @see net.sf.hajdbc.distributed.CommandDispatcher#getCoordinator()
	 */
	@Override
	public AddressMember getCoordinator()
	{
		Address address = this.getCoordinatorAddress();
		return (address != null) ? new AddressMember(address) : null;
	}

	private Address getCoordinatorAddress()
	{
		List<Address> members = this.dispatcher.getChannel().getView().getMembers();
		return members.isEmpty() ? null : members.get(0);
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.blocks.RequestHandler#handle(org.jgroups.Message)
	 */
	@Override
	public Object handle(Message message)
	{
		Command<Object, C> command = Objects.deserialize(message.getRawBuffer());

		this.logger.log(Level.DEBUG, Messages.COMMAND_RECEIVED.getMessage(command, message.getSrc()));
		
		return command.execute(this.context);
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MembershipListener#viewAccepted(org.jgroups.View)
	 */
	@Override
	public void viewAccepted(View view)
	{
		// notify DistributedLockManager, DistributedStateManager, or user-defined MembershipListener instances of added and deleted members
		if ((this.membershipListener != null) || (!this.userMembershipListeners.isEmpty()))
		{
			View oldView = this.viewReference.getAndSet(view);

			List<AddressMember> addedAddressMembers   = new ArrayList<AddressMember>();
			List<AddressMember> removedAddressMembers = new ArrayList<AddressMember>();

			// compute list of added members
			for (Address address: view.getMembers())
			{
				if ((oldView == null) || !oldView.containsMember(address))
				{
					addedAddressMembers.add(new AddressMember(address));
				}
			}
			
			// compute list of removed members
			if (oldView != null)
			{
				for (Address address: oldView.getMembers())
				{
					if (!view.containsMember(address))
					{
						removedAddressMembers.add(new AddressMember(address));
					}
				}
			}

			// notify DistributedLockManager or DistributedStateManager of added and deleted members
			if (this.membershipListener != null)
			{
				for (AddressMember addedAddressMember : addedAddressMembers)
				{
					this.membershipListener.added(addedAddressMember);
				}

				for (AddressMember removedAddressMember : removedAddressMembers)
				{
					this.membershipListener.removed(removedAddressMember);
				}
			}

			// notify user-defined MembershipListener instances of added and deleted members too
			for (MembershipListener userMembershipListener : this.userMembershipListeners)
			{
				for (AddressMember addedAddressMember : addedAddressMembers)
				{
					userMembershipListener.added(addedAddressMember);
				}

				for (AddressMember removedAddressMember : removedAddressMembers)
				{
					userMembershipListener.removed(removedAddressMember);
				}
			}
		}
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#getState(java.io.OutputStream)
	 */
	@Override
	public void getState(OutputStream output) throws Exception
	{
		ObjectOutput out = new ObjectOutputStream(output);
		this.stateful.writeState(out);
		out.flush();
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#setState(java.io.InputStream)
	 */
	@Override
	public void setState(InputStream input) throws Exception
	{
		this.stateful.readState(new ObjectInputStream(input));
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MembershipListener#suspect(org.jgroups.Address)
	 */
	@Override
	public void suspect(Address member)
	{
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MembershipListener#block()
	 */
	@Override
	public void block()
	{
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MembershipListener#unblock()
	 */
	@Override
	public void unblock()
	{
	}

	/**
	 * {@inheritDoc}
	 * @see org.jgroups.MessageListener#receive(org.jgroups.Message)
	 */
	@Override
	public void receive(Message message)
	{
	}

	private class RspCommandResponse<R> implements CommandResponse<R>
	{
		private final Rsp<R> response;
		
		public RspCommandResponse(Rsp<R> response)
		{
			this.response = response;
		}
		
		@Override
		public R get() throws ExecutionException
		{
			Throwable exception = this.response.getException();
			if (exception != null)
			{
				throw new ExecutionException(exception);
			}
			return this.response.getValue();
		}
	}

	private static class SimpleCommandResponse<R> implements CommandResponse<R>
	{
		private final R result;
		
		SimpleCommandResponse(R result)
		{
			this.result = result;
		}
		
		@Override
		public R get()
		{
			return this.result;
		}
	}

	private static class ExceptionCommandResponse<R> implements CommandResponse<R>
	{
		private final ExecutionException exception;
		
		ExceptionCommandResponse(ExecutionException exception)
		{
			this.exception = exception;
		}
		
		@Override
		public R get() throws ExecutionException
		{
			throw this.exception;
		}
	}
}
