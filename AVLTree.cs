﻿using System;

public class AVLTree<T> where T: class
{
    class Node
    {
        public int hash
        {
            get; private set;
        }

        public T item;

        public Node next, last; //for linked list
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
    private Node ListHead;

	public void Clear()
    {
        Root = null;
        ListHead = null;
    }

    public void Add(T item)
    {
        Node newNode = new Node(item);
        //linked list part
        newNode.next = ListHead;
        if (ListHead != null)
        {
            ListHead.last = newNode;
        }
        ListHead = newNode;
        //avl part
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
                //a new node is inserted, and balance operation is needed
                while(curNode != null)
                {
                    Node tmpNode = Balance(curNode);
                    curNode.height = Math.Max(curNode.lchild != null ? curNode.lchild.height : -1, curNode.rchild != null ? curNode.rchild.height : -1) + 1;
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
                    nextNode.parent.height = Math.Max(nextNode.parent.lchild != null ? nextNode.parent.lchild.height : -1, nextNode.parent.rchild != null ? nextNode.parent.rchild.height : -1) + 1;
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
                    curNode.parent.height = Math.Max(curNode.parent.lchild != null ? curNode.parent.lchild.height : -1, curNode.parent.rchild != null ? curNode.parent.rchild.height : -1) + 1;
                }

                while (balanceStart != null)
                {
                    Node tmpNode = Balance(balanceStart);
                    balanceStart.height = Math.Max(balanceStart.lchild != null ? balanceStart.lchild.height : -1, balanceStart.rchild != null ? balanceStart.rchild.height : -1) + 1;
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

        //linked list part
        if (curNode.last != null)
        {
            curNode.last.next = curNode.next;
        }
        else
        {
            ListHead = curNode.next;
        }

        if(curNode.next != null)
        {
            curNode.next.last = curNode.last;
        }

        curNode.last = curNode.next = curNode.lchild = curNode.rchild = curNode.parent = curNode.nextOpenHash = null;
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