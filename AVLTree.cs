using System;

public class AVLTree<T> where T: class
{
    class Node
    {
        public int hash
        {
            get; private set;
        }

        public T item;

        public Node parent, lchild, rchild;//for avl tree
        public Node nextOpenHash;
        public int height;

        public Node(T item)
        {
            this.item = item;
            hash = item.GetHashCode();
            height = 0;
        }

        public bool isLeftChild()
        {
            if(parent == null)
            {
                return false;
            }
            else
            {
                if(parent.lchild == this)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        public bool isLeaf()
        {
            return lchild == null && rchild == null;
        }

        public bool isFullNode()
        {
            return lchild != null && rchild != null;
        }
    }

    private Node Root;

	public void Clear()
    {
        Root = null;
    }

    public void Add(T item)
    {
        Node newNode = new Node(item);

        if(Root == null)
        {
            Root = newNode;
        }
        else
        {
            Node curNode = Root;
            bool crash = false;
            while(true)
            {
                if(curNode.hash > newNode.hash)
                {
                    if(curNode.lchild != null)
                    {
                        curNode = curNode.lchild;
                    }
                    else
                    {
                        curNode.lchild = newNode;
                        newNode.parent = curNode;
                        curNode.height++;
                        break;
                    }
                }
                else if(curNode.hash < newNode.hash)
                {
                    if(curNode.rchild != null)
                    {
                        curNode = curNode.rchild;
                    }
                    else
                    {
                        curNode.rchild = newNode;
                        newNode.parent = curNode;
                        curNode.height++;
                        break;
                    }
                }
                else
                {
                    //we have a hash crash, insert into the tail of open hash list
                    Node emptyList = curNode;
                    while (emptyList.nextOpenHash != null)
                    {
                        emptyList = emptyList.nextOpenHash;
                    }
                    emptyList.nextOpenHash = newNode;
                    crash = true;
                    break;
                }
            }

            if(!crash)
            {
                //no crash means a new node is inserted into tree, so balance operation is needed
                while(curNode != null)
                {
                    Node tmpNode = Balance(curNode);
                    curNode = tmpNode.parent;
                }
            }
        }
    }

    public void Remove(T item)
    {
        int hashCode = item.GetHashCode();

        Node curNode = Root;
        //find node in tree
        while(curNode != null)
        {
            if(curNode.hash > hashCode)
            {
                curNode = curNode.lchild;
            }
            else if(curNode.hash < hashCode)
            {
                curNode = curNode.rchild;
            }
            else
            {
                break;
            }
        }
        
        if(curNode == null)
        {
            //if node not in tree, return
            return;
        }

        if(curNode.item == item)
        {
            //if curNode is tree node
            if(curNode.nextOpenHash != null)
            {
                //use next item in open hash list to replace this one, no need to change tree structure
                curNode.nextOpenHash.parent = curNode.parent;
                curNode.nextOpenHash.lchild = curNode.lchild;
                curNode.nextOpenHash.rchild = curNode.rchild;
                curNode.nextOpenHash.height = curNode.height;
                if(curNode.parent == null)
                {
                    Root = curNode.nextOpenHash;
                }
                else
                {
                    bool isLeftChild = curNode.isLeftChild();
                    if(isLeftChild)
                    {
                        curNode.parent.lchild = curNode.nextOpenHash;
                    }
                    else
                    {
                        curNode.parent.rchild = curNode.nextOpenHash;
                    }
                }
            }
            else
            {
                Node balanceStart = null;
                //delete the node from the tree
                if(curNode.isLeaf())
                {
                    //leaf, delete from tree directly
                    if(curNode.parent == null)
                    {
                        Root = null;
                    }
                    else
                    {
                        if(curNode.isLeftChild())
                        {
                            curNode.parent.lchild = null;
                        }
                        else
                        {
                            curNode.parent.rchild = null;
                        }
                    }
                }
                else if(curNode.isFullNode())
                {
                    //with two children, find the next biggest item to replace this one
                    Node nextNode = curNode.rchild;
                    while(nextNode.lchild != null)
                    {
                        nextNode = nextNode.lchild;
                    }

                    balanceStart = nextNode.parent;
                    nextNode.parent.lchild = nextNode.rchild;
                    if(nextNode.rchild != null)
                        nextNode.rchild.parent = nextNode.parent;

                    nextNode.parent = curNode.parent;
                    nextNode.lchild = curNode.lchild;
                    nextNode.rchild = curNode.rchild;
                    if(curNode.parent == null)
                    {
                        Root = nextNode;
                    }
                    else
                    {
                        if(curNode.isLeftChild())
                        {
                            curNode.parent.lchild = nextNode;
                        }
                        else
                        {
                            curNode.parent.rchild = nextNode;
                        }
                    }

                    
                }
                else
                {
                    //only has one child
                    Node realChild = curNode.lchild != null ? curNode.lchild : curNode.rchild;
                    if(curNode.parent == null)
                    {
                        Root = realChild;
                    }
                    else
                    {
                        if(curNode.isLeftChild())
                        {
                            curNode.parent.lchild = realChild;
                        }
                        else
                        {
                            curNode.parent.rchild = realChild;
                        }
                    }
                    realChild.parent = curNode.parent;
                    balanceStart = curNode.parent;
                }

                while (balanceStart != null)
                {
                    Node tmpNode = Balance(balanceStart);
                    balanceStart = tmpNode.parent;
                }
                
            }
        }
        else
        {
            //if curNode is not tree node, search in open hash list
            Node lastNode = curNode;
            curNode = curNode.nextOpenHash;
            while(curNode != null)
            {
                if(curNode.item == item)
                {
                    //we found it! remove from linked list
                    lastNode.nextOpenHash = curNode.nextOpenHash;
                    break;
                }
                else
                {
                    lastNode = curNode;
                    curNode = curNode.nextOpenHash;
                }
            }
            
            if(curNode == null)
            {
                //if node is not in open hash list, return
                return;
            }
        }
    }

    private Node LeftRotate(Node node)
    {
        Node newNode = node.rchild;

        if(node.parent == null)
        {
            Root = newNode;
            newNode.parent = null;
        }
        else
        {
            bool isLeftChild = node.isLeftChild();
            if(isLeftChild)
            {
                node.parent.lchild = newNode;
            }
            else
            {
                node.parent.rchild = newNode;
            }
            newNode.parent = node.parent;
        }

        Node lastchild = newNode.lchild;
        newNode.lchild = node;
        node.parent = newNode;
        node.rchild = lastchild;
        if(lastchild != null)
            lastchild.parent = node;

        newNode.height = Math.Max(newNode.lchild != null ? newNode.lchild.height : -1, newNode.rchild != null ? newNode.rchild.height : -1) + 1;
        return newNode;
    }

    private Node RightRotate(Node node)
    {
        Node newNode = node.lchild;

        if (node.parent == null)
        {
            Root = newNode;
            newNode.parent = null;
        }
        else
        {
            bool isLeftChild = node.isLeftChild();
            if (isLeftChild)
            {
                node.parent.lchild = newNode;
            }
            else
            {
                node.parent.rchild = newNode;
            }
            newNode.parent = node.parent;
        }

        Node lastchild = newNode.rchild;
        newNode.rchild = node;
        node.parent = newNode;
        node.rchild = lastchild;
        if (lastchild != null)
            lastchild.parent = node;

        newNode.height = Math.Max(newNode.lchild != null ? newNode.lchild.height : -1, newNode.rchild != null ? newNode.rchild.height : -1) + 1;
        return newNode;
    }

    Node Balance(Node node)
    {
        int lheight = node.lchild != null ? node.lchild.height : -1;
        int rheight = node.rchild != null ? node.rchild.height : -1;
        if(lheight - rheight > 1)
        {
            int llheight = node.lchild.lchild != null ? node.lchild.lchild.height : -1;
            int lrheight = node.lchild.lchild != null ? node.lchild.lchild.height : -1;
            if(lrheight > llheight)
            {
                LeftRotate(node.lchild);
            }
            node = RightRotate(node);
        }
        else if(lheight - rheight < -1)
        {
            int rlheight = node.rchild.rchild != null ? node.rchild.rchild.height : -1;
            int rrheight = node.rchild.rchild != null ? node.rchild.rchild.height : -1;
            if (rlheight > rrheight)
            {
                RightRotate(node.rchild);
            }
            node = LeftRotate(node);
        }
        return node;
    }
}
