%% handle_call({link_op, NodeToLink, right, Level}, _From, State) ->
%%     ?L(),
%%     Self = self(),
%%     case right(State, Level) of
%%         [] ->
%%             ?L(),
%%             ?LOG(NodeToLink),
%%             gen_server:call(NodeToLink, {link_left_op, Level, Self}),
%%               ?L(),
%%               {reply, ok, set_right(State, Level, NodeToLink)};
%%         RightNode ->
%%             ?L(),
%%             {RightKey, _, _, _, _} = gen_server:call(RightNode, get_op),
%%             {NodeKey, _, _, _, _} = gen_server:call(NodeToLink, get_op),
%%             MyKey = State#state.key,
%%             if
%%                 RightKey < NodeKey ->
%%                     ?L(),
%%                     gen_server:call(RightNode, {link_op, NodeToLink, right, Level}),
%%                     {reply, ok, State};
%%                 true ->
%%                     ?L(),
%%                     gen_server:call(RightNode, {link_op, Self, left, Level}),
%%                     gen_server:call(NodeToLink, {link_left_op, Level, Self}),
%%                     ?L(),
%%                     {reply, ok, set_right(State, Level, NodeToLink)}
%%             end
%%     end;
%% handle_call({link_op, NodeToLink, left, Level}, _From, State) ->
%%     ?L(),
%%     Self = self(),
%%     case left(State, Level) of
%%         [] ->
%%             ?L(),
%%             gen_server:call(NodeToLink, {link_right_op, Level, Self}),
%%             {reply, ok, set_left(State, Level, NodeToLink)};
%%         LeftNode ->
%%             ?L(),
%%             {LeftKey, _, _, _, _} = gen_server:call(LeftNode, get_op),
%%             {NodeKey, _, _, _, _} = gen_server:call(NodeToLink, get_op),
%%             MyKey = State#state.key,
%%             if
%%                 LeftKey > NodeKey ->
%%                     ?L(),
%%                     gen_server:call(LeftNode, {link_op, NodeToLink, left, Level}),
%%                     {reply, ok, State};
%%                 true ->
%%                     ?L(),
%%                     gen_server:call(LeftNode, {link_op, Self, right, Level}),
%%                     gen_server:call(NodeToLink, {link_right_op, Level, Self}),
%%                    {reply, ok, set_left(State, Level, NodeToLink)}
%%             end
%%     end;

